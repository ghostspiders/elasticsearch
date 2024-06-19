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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.TransportResponse.Empty;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class JoinHelper {


    /**
     * 记录日志信息的Logger实例。
     */
    private static final Logger logger = LogManager.getLogger(JoinHelper.class);

    /**
     * 节点加入动作的名称，用于集群协调过程中的节点加入请求。
     */
    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";

    /**
     * 节点加入验证动作的名称，用于在集群协调过程中验证节点加入请求。
     */
    public static final String VALIDATE_JOIN_ACTION_NAME = "internal:cluster/coordination/join/validate";

    /**
     * 开始节点加入动作的名称，用于启动集群协调过程中的节点加入。
     */
    public static final String START_JOIN_ACTION_NAME = "internal:cluster/coordination/start_join";

    /**
     * 每个节点加入尝试的超时设置。
     * <p>
     * 这个设置定义了节点在尝试加入集群时等待响应的最长时间。
     */
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.join.timeout",
            TimeValue.timeValueMillis(60000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    /**
     * 主节点服务，用于处理与主节点相关的逻辑。
     */
    private final MasterService masterService;

    /**
     * 传输服务，负责节点间的通信。
     */
    private final TransportService transportService;

    /**
     * 加入任务执行器，用于执行与节点加入相关的任务。
     */
    private final JoinTaskExecutor joinTaskExecutor;

    /**
     * 加入超时时间，定义了节点加入尝试的超时时长。
     */
    private final TimeValue joinTimeout;

    /**
     * 待处理的外出节点加入请求集合，存储了节点对和加入请求的元组。
     * <p>
     * 集合是线程安全的，用于同步对外出加入请求的处理。
     */
    private final Set<Tuple<DiscoveryNode, JoinRequest>> pendingOutgoingJoins = Collections.synchronizedSet(new HashSet<>());

    /**
     * 原子引用，用于存储最后失败的节点加入尝试的信息。
     * <p>
     * 这可以用于记录和分析加入过程中的失败原因。
     */
    private AtomicReference<FailedJoinAttempt> lastFailedJoinAttempt = new AtomicReference<>();
    public JoinHelper(Settings settings, AllocationService allocationService, MasterService masterService,
                      TransportService transportService, LongSupplier currentTermSupplier, Supplier<ClusterState> currentStateSupplier,
                      BiConsumer<JoinRequest, JoinCallback> joinHandler, Function<StartJoinRequest, Join> joinLeaderInTerm,
                      Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators) {
        this.masterService = masterService;
        this.transportService = transportService;
        this.joinTimeout = JOIN_TIMEOUT_SETTING.get(settings);
        this.joinTaskExecutor = new JoinTaskExecutor(settings, allocationService, logger) {

            @Override
            public ClusterTasksResult<JoinTaskExecutor.Task> execute(ClusterState currentState, List<JoinTaskExecutor.Task> joiningTasks)
                throws Exception {
                // This is called when preparing the next cluster state for publication. There is no guarantee that the term we see here is
                // the term under which this state will eventually be published: the current term may be increased after this check due to
                // some other activity. That the term is correct is, however, checked properly during publication, so it is sufficient to
                // check it here on a best-effort basis. This is fine because a concurrent change indicates the existence of another leader
                // in a higher term which will cause this node to stand down.

                final long currentTerm = currentTermSupplier.getAsLong();
                if (currentState.term() != currentTerm) {
                    final CoordinationMetaData coordinationMetaData =
                        CoordinationMetaData.builder(currentState.coordinationMetaData()).term(currentTerm).build();
                    final MetaData metaData = MetaData.builder(currentState.metaData()).coordinationMetaData(coordinationMetaData).build();
                    currentState = ClusterState.builder(currentState).metaData(metaData).build();
                }
                return super.execute(currentState, joiningTasks);
            }

        };

        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false, JoinRequest::new,
            (request, channel, task) -> joinHandler.accept(request, transportJoinCallback(request, channel)));

        transportService.registerRequestHandler(MembershipAction.DISCOVERY_JOIN_ACTION_NAME, MembershipAction.JoinRequest::new,
            ThreadPool.Names.GENERIC, false, false,
            (request, channel, task) -> joinHandler.accept(new JoinRequest(request.getNode(), Optional.empty()), // treat as non-voting join
                transportJoinCallback(request, channel)));

        transportService.registerRequestHandler(START_JOIN_ACTION_NAME, Names.GENERIC, false, false,
            StartJoinRequest::new,
            (request, channel, task) -> {
                final DiscoveryNode destination = request.getSourceNode();
                sendJoinRequest(destination, Optional.of(joinLeaderInTerm.apply(request)));
                channel.sendResponse(Empty.INSTANCE);
            });

        transportService.registerRequestHandler(VALIDATE_JOIN_ACTION_NAME,
            ValidateJoinRequest::new, ThreadPool.Names.GENERIC,
            (request, channel, task) -> {
                final ClusterState localState = currentStateSupplier.get();
                if (localState.metaData().clusterUUIDCommitted() &&
                    localState.metaData().clusterUUID().equals(request.getState().metaData().clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException("join validation on cluster state" +
                        " with a different cluster uuid " + request.getState().metaData().clusterUUID() +
                        " than local cluster uuid " + localState.metaData().clusterUUID() + ", rejecting");
                }
                joinValidators.forEach(action -> action.accept(transportService.getLocalNode(), request.getState()));
                channel.sendResponse(Empty.INSTANCE);
            });

        transportService.registerRequestHandler(MembershipAction.DISCOVERY_JOIN_VALIDATE_ACTION_NAME,
            ValidateJoinRequest::new, ThreadPool.Names.GENERIC,
            (request, channel, task) -> {
                final ClusterState localState = currentStateSupplier.get();
                if (localState.metaData().clusterUUIDCommitted() &&
                    localState.metaData().clusterUUID().equals(request.getState().metaData().clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException("mixed-version cluster join validation on cluster state" +
                        " with a different cluster uuid " + request.getState().metaData().clusterUUID() +
                        " than local cluster uuid " + localState.metaData().clusterUUID()
                        + ", rejecting");
                }
                joinValidators.forEach(action -> action.accept(transportService.getLocalNode(), request.getState()));
                channel.sendResponse(Empty.INSTANCE);
            });

        transportService.registerRequestHandler(
            ZenDiscovery.DISCOVERY_REJOIN_ACTION_NAME, ZenDiscovery.RejoinClusterRequest::new, ThreadPool.Names.SAME,
            (request, channel, task) -> channel.sendResponse(Empty.INSTANCE)); // TODO: do we need to implement anything here?

        transportService.registerRequestHandler(
            MembershipAction.DISCOVERY_LEAVE_ACTION_NAME, MembershipAction.LeaveRequest::new, ThreadPool.Names.SAME,
            (request, channel, task) -> channel.sendResponse(Empty.INSTANCE)); // TODO: do we need to implement anything here?
    }

    /**
     * 创建并返回一个用于传输层加入请求的回调对象。
     * @param request 当前的传输请求。
     * @param channel 当前请求的传输通道。
     * @return 返回一个JoinCallback实例，用于处理加入请求的响应。
     */
    private JoinCallback transportJoinCallback(TransportRequest request, TransportChannel channel) {
        return new JoinCallback() {

            /**
             * 当加入请求成功时调用。
             */
            @Override
            public void onSuccess() {
                try {
                    // 发送空响应表示成功，使用Elasticsearch中的Empty实例。
                    channel.sendResponse(Empty.INSTANCE);
                } catch (IOException e) {
                    // 如果发送成功响应时发生异常，则调用onFailure处理。
                    onFailure(e);
                }
            }

            /**
             * 当加入请求失败时调用。
             * @param e 导致失败的异常。
             */
            @Override
            public void onFailure(Exception e) {
                try {
                    // 发送失败响应，将异常信息发送回请求方。
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    // 如果发送失败响应时再次发生异常，添加抑制异常并记录警告日志。
                    inner.addSuppressed(e);
                    logger.warn("failed to send back failure on join request", inner);
                }
            }

            /**
             * 返回回调的字符串表示，用于调试信息。
             * @return 回调的字符串描述。
             */
            @Override
            public String toString() {
                return "JoinCallback{request=" + request + "}";
            }
        };
    }
    /**
     * 检查是否有待处理的加入请求。
     * @return 如果存在待处理的加入请求，则返回true；否则返回false。
     */
    boolean isJoinPending() {
        // 检查pendingOutgoingJoins集合是否不为空，表示有加入请求正在处理中
        return pendingOutgoingJoins.isEmpty() == false;
    }

    /**
     * 发送节点加入请求到指定的目标节点。
     * @param destination 加入请求的目标节点。
     * @param optionalJoin 一个可选的Join对象，可能包含有关加入的额外信息。
     * @param <T> 返回类型。
     */
    public <T> void sendJoinRequest(DiscoveryNode destination, Optional<Join> optionalJoin) {
        // 调用重载的sendJoinRequest方法，传入空的回调函数
        sendJoinRequest(destination, optionalJoin, () -> {
            // 这里没有提供具体的回调逻辑，可能是一个占位符或默认行为
        });
    }

    /**
     * 用于记录节点加入尝试失败的详细信息的类。
     */
    static class FailedJoinAttempt {
        /**
         * 加入请求的目标节点。
         */
        private final DiscoveryNode destination;
        /**
         * 加入请求的详细信息。
         */
        private final JoinRequest joinRequest;
        /**
         * 加入过程中遇到的传输异常。
         */
        private final TransportException exception;
        /**
         * 加入尝试的时间戳，使用纳秒表示。
         */
        private final long timestamp;

        /**
         * 构造一个新的FailedJoinAttempt实例。
         * @param destination 加入请求的目标节点。
         * @param joinRequest 加入请求的详细信息。
         * @param exception 加入过程中遇到的异常。
         */
        FailedJoinAttempt(DiscoveryNode destination, JoinRequest joinRequest, TransportException exception) {
            this.destination = destination;
            this.joinRequest = joinRequest;
            this.exception = exception;
            this.timestamp = System.nanoTime();
        }

        /**
         * 记录加入失败的日志信息。
         */
        void logNow() {
            // 根据异常类型确定日志级别，并记录日志信息。
            logger.log(getLogLevel(exception),
                () -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest),
                exception);
        }

        /**
         * 根据异常类型获取相应的日志级别。
         * @param e 传输异常。
         * @return 日志级别。
         */
        static Level getLogLevel(TransportException e) {
            Throwable cause = e.unwrapCause();
            // 检查异常原因，并返回相应的日志级别。
            if (cause instanceof CoordinationStateRejectedException ||
                cause instanceof FailedToCommitClusterStateException ||
                cause instanceof NotMasterException) {
                return Level.DEBUG;
            }
            return Level.INFO;
        }

        /**
         * 使用时间戳记录一条警告级别的日志信息，显示最后一次失败的加入尝试。
         */
        void logWarnWithTimestamp() {
            // 计算从加入尝试到现在的时间差，并记录日志。
            logger.info(() -> new ParameterizedMessage("last failed join attempt was {} ago, failed to join {} with {}",
                    TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - timestamp)),
                    destination,
                    joinRequest),
                exception);
        }
    }

    void logLastFailedJoinAttempt() {
        FailedJoinAttempt attempt = lastFailedJoinAttempt.get();
        if (attempt != null) {
            attempt.logWarnWithTimestamp();
            lastFailedJoinAttempt.compareAndSet(attempt, null);
        }
    }

    /**
     * 发送节点加入请求到指定的目标节点。
     * @param destination 加入请求的目标节点。
     * @param optionalJoin 一个可选的Join对象，包含有关加入的额外信息。
     * @param onCompletion 请求发送后的回调操作。
     */
    public void sendJoinRequest(DiscoveryNode destination, Optional<Join> optionalJoin, Runnable onCompletion) {
        // 断言目标节点具有成为主节点的资格
        assert destination.isMasterNode() : "trying to join master-ineligible " + destination;
        // 创建一个新的加入请求
        final JoinRequest joinRequest = new JoinRequest(transportService.getLocalNode(), optionalJoin);
        // 创建一个用于去重的元组，包含目标节点和加入请求
        final Tuple<DiscoveryNode, JoinRequest> dedupKey = Tuple.tuple(destination, joinRequest);
        // 如果加入请求尚未发送，将其添加到待处理列表中
        if (pendingOutgoingJoins.add(dedupKey)) {
            // 记录尝试加入的日志信息
            logger.debug("attempting to join {} with {}", destination, joinRequest);
            // 根据目标节点的类型选择不同的动作名称和传输请求
            final String actionName;
            final TransportRequest transportRequest;
            if (Coordinator.isZen1Node(destination)) {
                actionName = MembershipAction.DISCOVERY_JOIN_ACTION_NAME;
                transportRequest = new MembershipAction.JoinRequest(transportService.getLocalNode());
            } else {
                actionName = JOIN_ACTION_NAME;
                transportRequest = joinRequest;
            }
            // 发送节点加入请求
            transportService.sendRequest(destination, actionName, transportRequest,
                TransportRequestOptions.builder().withTimeout(joinTimeout).build(),
                new TransportResponseHandler<Empty>() {
                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(Empty response) {
                        // 处理成功响应，从待处理列表中移除请求，并执行回调
                        pendingOutgoingJoins.remove(dedupKey);
                        logger.debug("successfully joined {} with {}", destination, joinRequest);
                        onCompletion.run();
                        lastFailedJoinAttempt.set(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        // 处理异常，从待处理列表中移除请求，并记录失败信息
                        pendingOutgoingJoins.remove(dedupKey);
                        logger.info(() -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest), exp);
                        onCompletion.run();
                        FailedJoinAttempt attempt = new FailedJoinAttempt(destination, joinRequest, exp);
                        attempt.logNow();
                        lastFailedJoinAttempt.set(attempt);
                    }

                    @Override
                    public String executor() {
                        // 指定响应处理的线程池
                        return Names.SAME;
                    }
                });
        } else {
            // 如果已经在尝试加入相同的节点和请求，则不发送重复请求
            logger.debug("already attempting to join {} with request {}, not sending request", destination, joinRequest);
        }
    }

    public void sendStartJoinRequest(final StartJoinRequest startJoinRequest, final DiscoveryNode destination) {
        assert startJoinRequest.getSourceNode().isMasterNode()
            : "sending start-join request for master-ineligible " + startJoinRequest.getSourceNode();
        transportService.sendRequest(destination, START_JOIN_ACTION_NAME,
            startJoinRequest, new TransportResponseHandler<Empty>() {
                @Override
                public Empty read(StreamInput in) {
                    return Empty.INSTANCE;
                }

                @Override
                public void handleResponse(Empty response) {
                    logger.debug("successful response to {} from {}", startJoinRequest, destination);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(new ParameterizedMessage("failure in response to {} from {}", startJoinRequest, destination), exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
    }

    /**
     * 发送验证节点加入请求。
     * @param node 要验证的节点。
     * @param state 当前的集群状态。
     * @param listener 响应或失败的监听器回调。
     */
    public void sendValidateJoinRequest(DiscoveryNode node, ClusterState state, ActionListener<TransportResponse.Empty> listener) {
        // 根据节点类型选择不同的动作名称
        final String actionName;
        if (Coordinator.isZen1Node(node)) {
            actionName = MembershipAction.DISCOVERY_JOIN_VALIDATE_ACTION_NAME;
        } else {
            actionName = VALIDATE_JOIN_ACTION_NAME;
        }

        // 创建验证加入请求
        transportService.sendRequest(node, actionName,
            new ValidateJoinRequest(state),
            // 设置请求的传输选项，包括加入超时
            TransportRequestOptions.builder().withTimeout(joinTimeout).build(),
            // 创建空响应处理器
            new EmptyTransportResponseHandler(ThreadPool.Names.GENERIC) {
                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    // 处理成功响应，调用监听器的onResponse方法
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    // 处理异常，调用监听器的onFailure方法
                    listener.onFailure(exp);
                }
            });
    }

    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        private final JoinTaskExecutor.Task task;
        private final JoinCallback joinCallback;

        JoinTaskListener(JoinTaskExecutor.Task task, JoinCallback joinCallback) {
            this.task = task;
            this.joinCallback = joinCallback;
        }

        @Override
        public void onFailure(String source, Exception e) {
            joinCallback.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            joinCallback.onSuccess();
        }

        @Override
        public String toString() {
            return "JoinTaskListener{task=" + task + "}";
        }
    }

    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback);

        default void close(Mode newMode) {
        }
    }

    class LeaderJoinAccumulator implements JoinAccumulator {
        /**
         * 处理来自其他节点的加入请求。
         * @param sender 请求加入的节点。
         * @param joinCallback 加入请求的回调。
         */
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            // 创建一个新的加入任务
            final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(sender, "join existing leader");
            // 将加入任务提交给主服务进行状态更新
            masterService.submitStateUpdateTask("node-join", task, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(task, joinCallback));
        }

        /**
         * 返回类的字符串表示。
         * @return 字符串表示。
         */
        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    static class InitialJoinAccumulator implements JoinAccumulator {
        /**
         * 处理初始化阶段的加入请求。
         * @param sender 请求加入的节点。
         * @param joinCallback 加入请求的回调。
         */
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            // 断言初始化阶段不应收到加入请求
            assert false : "unexpected join from " + sender + " during initialisation";
            // 如果收到请求，通过回调通知失败
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is not initialised yet"));
        }

        /**
         * 返回类的字符串表示。
         * @return 字符串表示。
         */
        @Override
        public String toString() {
            return "InitialJoinAccumulator";
        }
    }

    static class FollowerJoinAccumulator implements JoinAccumulator {
        /**
         * 处理来自其他节点的加入请求。
         * @param sender 请求加入的节点。
         * @param joinCallback 加入请求的回调。
         */
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            // 作为跟随者节点，拒绝所有加入请求
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        /**
         * 返回类的字符串表示。
         * @return 字符串表示。
         */
        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }

    class CandidateJoinAccumulator implements JoinAccumulator {

        // 用于存储来自不同节点的加入请求及其回调
        private final Map<DiscoveryNode, JoinCallback> joinRequestAccumulator = new HashMap<>();
        // 标记累积器是否已关闭
        boolean closed;

        /**
         * 处理来自特定节点的加入请求。
         * @param sender 加入请求的发送者节点。
         * @param joinCallback 加入请求的回调。
         */
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            // 断言累积器未关闭
            assert closed == false : "CandidateJoinAccumulator closed";
            // 存储或更新加入请求及其回调
            JoinCallback prev = joinRequestAccumulator.put(sender, joinCallback);
            if (prev != null) {
                // 如果已有旧的加入请求，则对其回调调用onFailure
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        /**
         * 关闭累积器，并根据新的角色处理累积的加入请求。
         * @param newMode 新的模式，是成为领导者还是跟随者。
         */
        @Override
        public void close(Mode newMode) {
            // 断言累积器未关闭
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            if (newMode == Mode.LEADER) {
                // 如果成为领导者，处理累积的加入请求
                final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new LinkedHashMap<>();
                joinRequestAccumulator.forEach((key, value) -> {
                    final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(key, "elect leader");
                    pendingAsTasks.put(task, new JoinTaskListener(task, value));
                });

                // 构建状态更新的来源描述
                final String stateUpdateSource = "elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";
                // 提交状态更新任务
                pendingAsTasks.put(JoinTaskExecutor.newBecomeMasterTask(), (source, e) -> {
                });
                pendingAsTasks.put(JoinTaskExecutor.newFinishElectionTask(), (source, e) -> {
                });
                masterService.submitStateUpdateTasks(stateUpdateSource, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor);
            } else {
                // 如果成为跟随者，对所有累积的加入请求调用onFailure
                assert newMode == Mode.FOLLOWER : newMode;
                joinRequestAccumulator.values().forEach(joinCallback -> joinCallback.onFailure(
                    new CoordinationStateRejectedException("became follower")));
            }
            // CandidateJoinAccumulator仅在成为领导者或跟随者时关闭，其他情况下累积收到的所有加入请求
            // 无论任期如何。
        }

        /**
         * 返回类的字符串表示，用于调试信息。
         * @return 字符串表示。
         */
        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() +
                ", closed=" + closed + '}';
        }
    }
}
