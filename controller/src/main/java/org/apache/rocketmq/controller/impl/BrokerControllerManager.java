/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.controller.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.header.NotifyBrokerRoleChangedRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

public class BrokerControllerManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private final Controller controller;
    private final ControllerConfig controllerConfig;
    private final DefaultBrokerHeartbeatManager heartbeatManager;

    private final ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerControllerManager_scheduledService_"));
    private final ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactoryImpl("BrokerControllerManager_executorService_"));

    private final RemotingClient remotingClient;

    public BrokerControllerManager(Controller controller, BrokerHeartbeatManager heartbeatManager,
        ControllerConfig controllerConfig,
        NettyClientConfig nettyClientConfig) {
        this.controller = controller;
        this.heartbeatManager = (DefaultBrokerHeartbeatManager) heartbeatManager;
        this.controllerConfig = controllerConfig;
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
    }

    public void start() {
        this.remotingClient.start();
        this.scheduledService.scheduleWithFixedDelay(this::scanNotActiveMaster, 2000, this.controllerConfig.getScanNotActiveMasterInterval(), TimeUnit.MILLISECONDS);
    }

    public void scanNotActiveMaster() {
        try {
            if (this.controller.isLeaderState()) {
                log.info("start scanNotActiveMaster.");
                for (Map.Entry<String, BrokerData> next : this.heartbeatManager.getBrokerAddrTable().entrySet()) {
                    final String clusterName = next.getValue().getCluster();
                    final Map<Long, String> brokerAddrs = next.getValue().getBrokerAddrs();
                    if (brokerAddrs.isEmpty()) {
                        continue;
                    }
                    final String brokerAddr = brokerAddrs.get(MixAll.MASTER_ID);
                    if (StringUtils.isEmpty(brokerAddr) || !this.heartbeatManager.isBrokerActive(clusterName, brokerAddr)) {
                        handleMasterInactive(clusterName, next.getKey(), brokerAddr);
                    }
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveMaster exception", e);
        }
    }

    /**
     * When detects the "Broker is not active",
     * we call this method to elect a master and do something else.
     *
     * @param clusterName   The cluster name of this inactive broker
     * @param brokerName    The inactive broker name
     * @param brokerAddress The inactive broker address(ip)
     */
    public void handleMasterInactive(String clusterName, String brokerName, String brokerAddress) {
        final CompletableFuture<RemotingCommand> future = controller.electMaster(new ElectMasterRequestHeader(brokerName));
        try {
            final RemotingCommand response = future.get(5, TimeUnit.SECONDS);
            final ElectMasterResponseHeader responseHeader = (ElectMasterResponseHeader) response.readCustomHeader();
            if (responseHeader != null) {
                log.info("Broker {}'s master {} shutdown, elect a new master done, result:{}", brokerName, brokerAddress, responseHeader);
                if (StringUtils.isNotEmpty(responseHeader.getNewMasterAddress())) {
                    heartbeatManager.changeBrokerMetadata(clusterName, responseHeader.getNewMasterAddress(), MixAll.MASTER_ID);
                }
                if (controllerConfig.isNotifyBrokerRoleChanged()) {
                    notifyBrokerRoleChanged(responseHeader, clusterName);
                }
            }
        } catch (Exception e) {
            log.error("Handle master inactive error", e);
        }
    }

    /**
     * Notify master and all slaves for a broker that the master role changed.
     */
    public void notifyBrokerRoleChanged(final ElectMasterResponseHeader electMasterResult, final String clusterName) {
        final BrokerMemberGroup memberGroup = electMasterResult.getBrokerMemberGroup();
        if (memberGroup != null) {
            // First, inform the master
            final String master = electMasterResult.getNewMasterAddress();
            if (StringUtils.isNoneEmpty(master) && this.heartbeatManager.isBrokerActive(clusterName, master)) {
                doNotifyBrokerRoleChanged(master, MixAll.MASTER_ID, electMasterResult);
            }

            // Then, inform all slaves
            final Map<Long, String> brokerIdAddrs = memberGroup.getBrokerAddrs();
            for (Map.Entry<Long, String> broker : brokerIdAddrs.entrySet()) {
                if (!broker.getValue().equals(master) && this.heartbeatManager.isBrokerActive(clusterName, broker.getValue())) {
                    doNotifyBrokerRoleChanged(broker.getValue(), broker.getKey(), electMasterResult);
                }
            }
        }
    }

    public void doNotifyBrokerRoleChanged(final String brokerAddr, final Long brokerId,
        final ElectMasterResponseHeader responseHeader) {
        if (StringUtils.isNoneEmpty(brokerAddr)) {
            log.info("Try notify broker {} with id {} that role changed, responseHeader:{}", brokerAddr, brokerId, responseHeader);
            final NotifyBrokerRoleChangedRequestHeader requestHeader = new NotifyBrokerRoleChangedRequestHeader(responseHeader.getNewMasterAddress(),
                responseHeader.getMasterEpoch(), responseHeader.getSyncStateSetEpoch(), brokerId);
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_BROKER_ROLE_CHANGED, requestHeader);
            try {
                this.remotingClient.invokeSync(brokerAddr, request, 10000);
            } catch (final Exception e) {
                log.error("Failed to notify broker {} with id {} that role changed", brokerAddr, brokerId, e);
            }
        }
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.scheduledService.shutdown();
        this.executor.shutdown();
    }

}
