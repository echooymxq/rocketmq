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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class BrokerControllerManager {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private final ScheduledExecutorService scheduledService = new ScheduledThreadPoolExecutor(2,
        new ThreadFactoryImpl("BrokerControllerManager_scheduledService_"));

    private final Controller controller;
    private final DefaultBrokerHeartbeatManager brokerHeartbeatManager;
    private final Map<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    private final Map<String/* brokerName */, BrokerData> brokerAddrTable;

    public BrokerControllerManager(Controller controller, DefaultBrokerHeartbeatManager manager) {
        this.controller = controller;
        this.brokerHeartbeatManager = manager;
        this.clusterAddrTable = new ConcurrentHashMap<>(32);
        this.brokerAddrTable = new ConcurrentHashMap<>(128);
    }

    public void start() {
        this.scheduledService.scheduleWithFixedDelay(this::scanBrokerHealth, 2000, 10 * 1000, TimeUnit.MILLISECONDS);
    }

    public void scanBrokerHealth() {
        if (controller.isLeaderState()) {
            LOG.info("start scanBrokerHealth.");
            for (Map.Entry<String, Set<String>> entry : this.clusterAddrTable.entrySet()) {
                String clusterName = entry.getKey();
                for (String brokerName : entry.getValue()) {
                    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                    if (brokerData == null) {
                        LOG.warn("Broker[{}] not exist.", brokerName);
                        continue;
                    }
                    String brokerAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    boolean isBrokerHealth = false;
                    if (brokerAddr != null) {
                        if (brokerHeartbeatManager.isBrokerActive(clusterName, brokerAddr)) {
                            continue;
                        }
                    } else {
                        LOG.warn("Broker[{}] master broker not exist.", brokerName);
                    }
                    try {
                        if (!isBrokerHealth) {
                            this.brokerHeartbeatManager.notifyBrokerInActive(clusterName, brokerName, brokerAddr, MixAll.MASTER_ID);
                        }
                    } catch (Exception e) {
                        LOG.error("Switch Broker[{}] master-slave error.", brokerName, e);
                    }
                }
            }
        }
    }

    public void registerBroker(String clusterName, String brokerName, String brokerAddr,
        long brokerId, Long timeoutMillis, Channel channel, Integer epoch, Long maxOffset) {
        Set<String> brokerNames = this.clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>());
        brokerNames.add(brokerName);
        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (null == brokerData) {
            brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
            this.brokerAddrTable.put(brokerName, brokerData);
        }
        Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
        //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
        //The same IP:PORT must only have one record in brokerAddrTable
        Iterator<Map.Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, String> item = it.next();
            if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                it.remove();
            }
        }

        this.brokerHeartbeatManager.registerBroker(clusterName, brokerName, brokerAddr,
            brokerId, timeoutMillis, channel, epoch, maxOffset);
    }

    public void stop() {
        this.scheduledService.shutdown();
    }

}
