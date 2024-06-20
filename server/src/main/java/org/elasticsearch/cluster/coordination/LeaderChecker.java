/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.MasterFaultDetection;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 LeaderChecker：这是一个负责监控领导者状态的组件，确保领导者节点仍然处于活跃状态。
 */
public class LeaderChecker {

    /**
     * 初始化日志记录器。
     */
    private static final Logger logger = LogManager.getLogger(LeaderChecker.class);

    /**
     * 领导者检查动作的名称。
     */
    public static final String LEADER_CHECK_ACTION_NAME = "internal:coordination/fault_detection/leader_check";

    /**
     * 向领导者发送检查之间的时间间隔设置。
     */
    public static final Setting<TimeValue> LEADER_CHECK_INTERVAL_SETTING =
        Setting.timeSetting("cluster.fault_detection.leader_check.interval",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(100), Setting.Property.NodeScope);

    /**
     * 向领导者发送的每次检查的超时时间设置。
     */
    public static final Setting<TimeValue> LEADER_CHECK_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.fault_detection.leader_check.timeout",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    /**
     * 领导者被认为失败之前必须发生的失败检查次数设置。
     */
    public static final Setting<Integer> LEADER_CHECK_RETRY_COUNT_SETTING =
        Setting.intSetting("cluster.fault_detection.leader_check.retry_count", 3, 1, Setting.Property.NodeScope);

    /**
     * 系统设置。
     */
    private final Settings settings;

    /**
     * 领导者检查间隔。
     */
    private final TimeValue leaderCheckInterval;

    /**
     * 领导者检查超时时间。
     */
    private final TimeValue leaderCheckTimeout;

    /**
     * 领导者检查重试次数。
     */
    private final int leaderCheckRetryCount;

    /**
     * 传输服务，用于节点间的消息传递。
     */
    private final TransportService transportService;

    /**
     * 领导者故障时调用的Runnable。
     */
    private final Runnable onLeaderFailure;

    /**
     * 当前检查调度器的原子引用。
     */
    private AtomicReference<CheckScheduler> currentChecker = new AtomicReference<>();

    /**
     * 集群中节点的动态信息。
     */
    private volatile DiscoveryNodes discoveryNodes;

    /**
     * LeaderChecker类的构造方法，用于初始化领导者检查器。
     *
     * @param settings 系统设置，包含领导者检查相关的配置。
     * @param transportService 传输服务，用于节点间的消息传递。
     * @param onLeaderFailure 领导者发生故障时调用的Runnable。
     */
    public LeaderChecker(final Settings settings, final TransportService transportService, final Runnable onLeaderFailure) {
        // 保存传入的设置、传输服务和领导者故障处理Runnable。
        this.settings = settings;
        leaderCheckInterval = LEADER_CHECK_INTERVAL_SETTING.get(settings); // 获取领导者检查间隔设置。
        leaderCheckTimeout = LEADER_CHECK_TIMEOUT_SETTING.get(settings); // 获取领导者检查超时设置。
        leaderCheckRetryCount = LEADER_CHECK_RETRY_COUNT_SETTING.get(settings); // 获取领导者检查重试次数设置。
        this.transportService = transportService;
        this.onLeaderFailure = onLeaderFailure;

        // 注册领导者检查请求处理器。
        transportService.registerRequestHandler(
            LEADER_CHECK_ACTION_NAME, Names.SAME, false, false, LeaderCheckRequest::new,
            (request, channel, task) -> {
                handleLeaderCheck(request); // 处理领导者检查请求。
                channel.sendResponse(Empty.INSTANCE); // 发送空响应。
            });

        // 注册主节点心跳请求处理器。
        transportService.registerRequestHandler(
            MasterFaultDetection.MASTER_PING_ACTION_NAME, MasterFaultDetection.MasterPingRequest::new,
            Names.SAME, false, false, (request, channel, task) -> {
                try {
                    handleLeaderCheck(new LeaderCheckRequest(request.sourceNode)); // 处理领导者检查。
                } catch (CoordinationStateRejectedException e) {
                    // 如果不是当前主节点，抛出异常。
                    throw new MasterFaultDetection.ThisIsNotTheMasterYouAreLookingForException(e.getMessage());
                }
                channel.sendResponse(new MasterFaultDetection.MasterPingResponseResponse()); // 发送心跳响应。
            });

        // 添加连接监听器到传输服务。
        transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                // 当节点断开连接时调用。
                handleDisconnectedNode(node);
            }
        });
    }

    /**
     * 获取当前的领导者节点。
     * 如果当前没有领导者检查器在运行，则返回null。
     *
     * @return 可选的领导者节点，如果存在领导者检查器则包含领导者节点，否则为空。
     */
    public DiscoveryNode leader() {
        CheckScheduler checkScheduler = currentChecker.get();
        // 如果检查调度器为空，则返回null；否则返回检查调度器中的领导者节点。
        return checkScheduler == null ? null : checkScheduler.leader;
    }

    /**
     * 启动和/或停止给定领导者的领导者检查器。
     * 应该仅在成功加入此领导者后才调用此方法。
     *
     * @param leader 要检查的领导者节点，如果是null，则禁用检查。
     */
    public void updateLeader(@Nullable final DiscoveryNode leader) {
        // 断言本地节点不能是领导者。
        assert transportService.getLocalNode().equals(leader) == false;
        final CheckScheduler checkScheduler;
        if (leader != null) {
            // 如果传入的领导者不为空，创建一个新的检查调度器。
            checkScheduler = new CheckScheduler(leader);
        } else {
            // 如果传入的领导者为null，设置检查调度器为空。
            checkScheduler = null;
        }
        // 获取并设置当前的检查调度器。
        CheckScheduler previousChecker = currentChecker.getAndSet(checkScheduler);
        // 如果存在先前的检查调度器，关闭它。
        if (previousChecker != null) {
            previousChecker.close();
        }
        // 如果新的检查调度器不为空，处理唤醒事件。
        if (checkScheduler != null) {
            checkScheduler.handleWakeUp();
        }
    }

    /**
     * 更新“已知”的发现节点。
     * 在领导者发布新的集群状态之前应该调用此方法，以反映新的发布目标，并且在领导者变为非领导者时也应该调用。
     *  TODO 如果心跳可以使节点变成跟随者，则在向新节点发送心跳之前也需要调用此方法。
     * <p>
     * isLocalNodeElectedMaster() 应该反映此节点是否是领导者，nodeExists() 应该表示节点是否是已知的发布目标。
     *
     * @param discoveryNodes 要设置的发现节点。
     */
    public void setCurrentNodes(DiscoveryNodes discoveryNodes) {
        logger.trace("setCurrentNodes: {}", discoveryNodes);
        this.discoveryNodes = discoveryNodes;
    }

    /**
     * 仅用于断言：检查当前节点是否是主节点。
     * 此方法用于确保当前节点是否被选举为领导者。
     *
     * @return 如果当前节点是主节点，则返回true。
     */
    boolean currentNodeIsMaster() {
        return discoveryNodes.isLocalNodeElectedMaster();
    }

    /**
     * 处理来自领导者的检查请求。
     * 当收到领导者检查请求时调用此方法，以验证请求是否来自已知的领导者。
     *
     * @param request 领导者检查请求。
     */
    private void handleLeaderCheck(LeaderCheckRequest request) {
        final DiscoveryNodes discoveryNodes = this.discoveryNodes;
        assert discoveryNodes != null;

        if (discoveryNodes.isLocalNodeElectedMaster() == false) {
            logger.debug("non-master handling {}", request);
            throw new CoordinationStateRejectedException("non-leader rejecting leader check");
        } else if (discoveryNodes.nodeExists(request.getSender()) == false) {
            logger.debug("leader check from unknown node: {}", request);
            throw new CoordinationStateRejectedException("leader check from unknown node");
        } else {
            logger.trace("handling {}", request);
        }
    }

    /**
     * 处理节点断开连接的事件。
     * 当集群中的节点断开连接时调用此方法，以更新领导者检查器的状态。
     *
     * @param discoveryNode 断开连接的节点。
     */
    private void handleDisconnectedNode(DiscoveryNode discoveryNode) {
        CheckScheduler checkScheduler = currentChecker.get();
        if (checkScheduler != null) {
            checkScheduler.handleDisconnectedNode(discoveryNode);
        } else {
            logger.trace("disconnect event ignored for {}, no check scheduler", discoveryNode);
        }
    }
    /**
     * 用于调度和执行领导者检查的内部类。
     * 实现了Releasable接口，以确保资源可以被正确释放。
     */
    private class CheckScheduler implements Releasable {

        // 用于标记检查调度器是否已关闭的原子布尔变量。
        private final AtomicBoolean isClosed = new AtomicBoolean();
        // 自上次成功以来的失败次数。
        private final AtomicLong failureCountSinceLastSuccess = new AtomicLong();
        // 要检查的领导者节点。
        private final DiscoveryNode leader;

        CheckScheduler(final DiscoveryNode leader) {
            this.leader = leader;
        }

        @Override
        public void close() {
            if (isClosed.compareAndSet(false, true) == false) {
                logger.trace("already closed, doing nothing");
            } else {
                logger.debug("closed");
            }
        }
        /**
         * 处理唤醒事件，执行领导者检查。
         */
        void handleWakeUp() {
            if (isClosed.get()) {
                logger.trace("closed check scheduler woken up, doing nothing");
                return;
            }

            logger.trace("checking {} with [{}] = {}", leader, LEADER_CHECK_TIMEOUT_SETTING.getKey(), leaderCheckTimeout);

            final String actionName;
            final TransportRequest transportRequest;
            if (Coordinator.isZen1Node(leader)) {
                actionName = MasterFaultDetection.MASTER_PING_ACTION_NAME;
                transportRequest = new MasterFaultDetection.MasterPingRequest(
                    transportService.getLocalNode(), leader, ClusterName.CLUSTER_NAME_SETTING.get(settings));
            } else {
                actionName = LEADER_CHECK_ACTION_NAME;
                transportRequest = new LeaderCheckRequest(transportService.getLocalNode());
            }
            // TODO lag detection:
            // In the PoC, the leader sent its current version to the follower in the response to a LeaderCheck, so the follower
            // could detect if it was lagging. We'd prefer this to be implemented on the leader, so the response is just
            // TransportResponse.Empty here.
            transportService.sendRequest(leader, actionName, transportRequest,
                TransportRequestOptions.builder().withTimeout(leaderCheckTimeout).withType(Type.PING).build(),

                new TransportResponseHandler<TransportResponse.Empty>() {

                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(Empty response) {
                        if (isClosed.get()) {
                            logger.debug("closed check scheduler received a response, doing nothing");
                            return;
                        }

                        failureCountSinceLastSuccess.set(0);
                        scheduleNextWakeUp(); // logs trace message indicating success
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (isClosed.get()) {
                            logger.debug("closed check scheduler received a response, doing nothing");
                            return;
                        }

                        if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                            logger.debug(new ParameterizedMessage("leader [{}] disconnected, failing immediately", leader), exp);
                            leaderFailed();
                            return;
                        }

                        long failureCount = failureCountSinceLastSuccess.incrementAndGet();
                        if (failureCount >= leaderCheckRetryCount) {
                            logger.debug(new ParameterizedMessage("{} consecutive failures (limit [{}] is {}) so leader [{}] has failed",
                                failureCount, LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), leaderCheckRetryCount, leader), exp);
                            leaderFailed();
                            return;
                        }

                        logger.debug(new ParameterizedMessage("{} consecutive failures (limit [{}] is {}) with leader [{}]",
                            failureCount, LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), leaderCheckRetryCount, leader), exp);
                        scheduleNextWakeUp();
                    }

                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                });
        }
        /**
         * 处理领导者失败的情况。
         */
        void leaderFailed() {
            if (isClosed.compareAndSet(false, true)) {
                transportService.getThreadPool().generic().execute(onLeaderFailure);
            } else {
                logger.trace("already closed, not failing leader");
            }
        }
        /**
         * 处理断开连接的节点。
         *
         * @param discoveryNode 断开连接的节点。
         */
        void handleDisconnectedNode(DiscoveryNode discoveryNode) {
            if (discoveryNode.equals(leader)) {
                leaderFailed();
            }
        }
        /**
         * 安排下一次唤醒检查。
         */
        private void scheduleNextWakeUp() {
            logger.trace("scheduling next check of {} for [{}] = {}", leader, LEADER_CHECK_INTERVAL_SETTING.getKey(), leaderCheckInterval);
            transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    handleWakeUp();
                }

                @Override
                public String toString() {
                    return "scheduled check of leader " + leader;
                }
            }, leaderCheckInterval, Names.SAME);
        }
    }

    public static class LeaderCheckRequest extends TransportRequest {

        private final DiscoveryNode sender;

        public LeaderCheckRequest(final DiscoveryNode sender) {
            this.sender = sender;
        }

        public LeaderCheckRequest(final StreamInput in) throws IOException {
            super(in);
            sender = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            sender.writeTo(out);
        }

        public DiscoveryNode getSender() {
            return sender;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final LeaderCheckRequest that = (LeaderCheckRequest) o;
            return Objects.equals(sender, that.sender);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sender);
        }

        @Override
        public String toString() {
            return "LeaderCheckRequest{" +
                "sender=" + sender +
                '}';
        }
    }
}

