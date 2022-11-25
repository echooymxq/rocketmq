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

import io.netty.channel.Channel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BrokerAddrInfo;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.BrokerLiveInfo;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

public class DefaultBrokerHeartbeatManager implements BrokerHeartbeatManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 10;
    private final ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_scheduledService_"));

    private final ControllerConfig controllerConfig;
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    private final Map<String/* brokerName */, BrokerData> brokerAddrTable;

    public DefaultBrokerHeartbeatManager(final ControllerConfig controllerConfig) {
        this.controllerConfig = controllerConfig;
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        this.brokerAddrTable = new ConcurrentHashMap<>(256);
    }

    @Override
    public void start() {
        this.scheduledService.scheduleAtFixedRate(this::scanNotActiveBroker, 2000, this.controllerConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        this.scheduledService.shutdown();
    }

    public void scanNotActiveBroker() {
        try {
            final Iterator<Map.Entry<BrokerAddrInfo, BrokerLiveInfo>> iterator = this.brokerLiveTable.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<BrokerAddrInfo, BrokerLiveInfo> next = iterator.next();
                BrokerLiveInfo brokerLiveInfo = next.getValue();
                long last = brokerLiveInfo.getLastUpdateTimestamp();
                long timeoutMillis = brokerLiveInfo.getHeartbeatTimeoutMillis();
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    final Channel channel = brokerLiveInfo.getChannel();
                    iterator.remove();
                    if (channel != null) {
                        RemotingHelper.closeChannel(channel);
                    }
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    @Override
    public void changeBrokerMetadata(String clusterName, String brokerAddr, Long brokerId) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            prev.setBrokerId(brokerId);
            log.info("Change broker {}'s brokerId to {}", brokerAddr, brokerId);
        }
    }

    @Override
    public void registerBroker(String clusterName, String brokerName, String brokerAddr,
        long brokerId, Long timeoutMillis, Channel channel, Integer epoch, Long maxOffset, final Integer electionPriority) {
        final BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        final BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(addrInfo,
            new BrokerLiveInfo(brokerName,
                brokerAddr,
                brokerId,
                System.currentTimeMillis(),
                timeoutMillis == null ? DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis,
                channel,
                epoch == null ? -1 : epoch,
                maxOffset == null ? -1 : maxOffset,
                electionPriority == null ? Integer.MAX_VALUE : electionPriority));
        if (prevBrokerLiveInfo == null) {
            log.info("new broker registered, {}, brokerId:{}", addrInfo, brokerId);
        }
    }

    @Override
    public void onBrokerHeartbeat(final String clusterName, final String brokerName, final Long brokerId, final String brokerAddr,
        final Integer epoch, final Long maxOffset, final Long confirmOffset) {

        BrokerData brokerData = brokerAddrTable.computeIfAbsent(brokerName, name -> new BrokerData(clusterName, brokerName, new HashMap<>()));
        //Switch slave to master: first remove <1, IP:PORT>, then add <0, IP:PORT>
        brokerData.getBrokerAddrs().entrySet().removeIf(broker -> broker.getValue().equals(brokerAddr) && !broker.getKey().equals(brokerId));
        brokerData.getBrokerAddrs().put(brokerId, brokerAddr);

        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (null == prev) {
            return;
        }
        int realEpoch = Optional.ofNullable(epoch).orElse(-1);
        long realMaxOffset = Optional.ofNullable(maxOffset).orElse(-1L);
        long realConfirmOffset = Optional.ofNullable(confirmOffset).orElse(-1L);

        prev.setLastUpdateTimestamp(System.currentTimeMillis());
        prev.setBrokerId(brokerId);
        if (realEpoch > prev.getEpoch() || realEpoch == prev.getEpoch() && realMaxOffset > prev.getMaxOffset()) {
            prev.setEpoch(realEpoch);
            prev.setMaxOffset(realMaxOffset);
            prev.setConfirmOffset(realConfirmOffset);
        }
    }

    @Override
    public void onBrokerChannelClose(Channel channel) {
        for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
            if (entry.getValue().getChannel() == channel) {
                BrokerAddrInfo brokerAddrInfo = entry.getKey();
                BrokerLiveInfo brokerLiveInfo = entry.getValue();
                log.info("Channel {} inactive, broker {}, addr:{}, id:{}", brokerLiveInfo.getChannel(), brokerLiveInfo.getBrokerName(),
                    brokerAddrInfo.getBrokerAddr(), brokerLiveInfo.getBrokerId());

                removeBrokerAddr(brokerLiveInfo.getBrokerName(), brokerLiveInfo.getBrokerAddr());
                this.brokerLiveTable.remove(brokerAddrInfo);
                break;
            }
        }
    }

    @Override
    public BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerAddr) {
        return this.brokerLiveTable.get(new BrokerAddrInfo(clusterName, brokerAddr));
    }

    public Map<String, BrokerData> getBrokerAddrTable() {
        return brokerAddrTable;
    }

    private void removeBrokerAddr(final String brokerName, final String brokerAddr) {
        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        brokerData.getBrokerAddrs().entrySet().removeIf(broker -> broker.getValue().equals(brokerAddr));
    }

    @Override
    public boolean isBrokerActive(String clusterName, String brokerAddr) {
        BrokerLiveInfo info = this.brokerLiveTable.get(new BrokerAddrInfo(clusterName, brokerAddr));
        if (info != null) {
            long last = info.getLastUpdateTimestamp();
            long timeoutMillis = info.getHeartbeatTimeoutMillis();
            return (last + timeoutMillis) >= System.currentTimeMillis();
        }
        return false;
    }

}
