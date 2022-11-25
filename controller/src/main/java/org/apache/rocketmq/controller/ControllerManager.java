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
package org.apache.rocketmq.controller;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.BrokerControllerManager;
import org.apache.rocketmq.controller.impl.DLedgerController;
import org.apache.rocketmq.controller.impl.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RequestCode;

public class ControllerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private final ControllerConfig controllerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final BrokerHousekeepingService brokerHousekeepingService;
    private final Configuration configuration;
    private Controller controller;
    private BrokerControllerManager brokerControllerManager;
    private ExecutorService controllerRequestExecutor;
    private BlockingQueue<Runnable> controllerRequestThreadPoolQueue;
    private BrokerHeartbeatManager heartbeatManager;

    public ControllerManager(ControllerConfig controllerConfig, NettyServerConfig nettyServerConfig,
        NettyClientConfig nettyClientConfig) {
        this.controllerConfig = controllerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(log, this.controllerConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.controllerConfig, "configStorePath");
    }

    public boolean initialize() {
        this.controllerRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.controllerConfig.getControllerRequestThreadPoolQueueCapacity());
        this.controllerRequestExecutor = new ThreadPoolExecutor(
                this.controllerConfig.getControllerThreadPoolNums(),
                this.controllerConfig.getControllerThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.controllerRequestThreadPoolQueue,
                new ThreadFactoryImpl("ControllerRequestExecutorThread_")) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
                return new FutureTaskExt<>(runnable, value);
            }
        };
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(this.controllerConfig);
        if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerPeers())) {
            throw new IllegalArgumentException("Attribute value controllerDLegerPeers of ControllerConfig is null or empty");
        }
        if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerSelfId())) {
            throw new IllegalArgumentException("Attribute value controllerDLegerSelfId of ControllerConfig is null or empty");
        }
        this.controller = new DLedgerController(this.controllerConfig, this.heartbeatManager::isBrokerActive,
                this.nettyServerConfig, this.nettyClientConfig, this.brokerHousekeepingService,
                new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo));

        this.brokerControllerManager = new BrokerControllerManager(controller, heartbeatManager, controllerConfig, nettyClientConfig);

        registerProcessor();
        return true;
    }

    public void registerProcessor() {
        final ControllerRequestProcessor controllerRequestProcessor = new ControllerRequestProcessor(this);
        final RemotingServer controllerRemotingServer = this.controller.getRemotingServer();
        assert controllerRemotingServer != null;
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_ELECT_MASTER, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_REGISTER_BROKER, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_REPLICA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_METADATA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.BROKER_HEARTBEAT, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.UPDATE_CONTROLLER_CONFIG, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.GET_CONTROLLER_CONFIG, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CLEAN_BROKER_DATA, controllerRequestProcessor, this.controllerRequestExecutor);
    }

    public void start() {
        this.heartbeatManager.start();
        this.controller.startup();
        this.brokerControllerManager.start();
    }

    public void shutdown() {
        this.heartbeatManager.shutdown();
        this.controllerRequestExecutor.shutdown();
        this.brokerControllerManager.shutdown();
        this.controller.shutdown();
    }

    public BrokerControllerManager getBrokerControllerManager() {
        return brokerControllerManager;
    }

    public BrokerHeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    public ControllerConfig getControllerConfig() {
        return controllerConfig;
    }

    public Controller getController() {
        return controller;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
