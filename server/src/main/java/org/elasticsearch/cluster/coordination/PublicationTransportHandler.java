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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.zen.PublishClusterStateAction;
import org.elasticsearch.discovery.zen.PublishClusterStateStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class PublicationTransportHandler {

    /**
     * 日志记录器。
     */
    private static final Logger logger = LogManager.getLogger(PublicationTransportHandler.class);

    /**
     * 发布状态操作的名称。
     */
    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";

    /**
     * 提交状态操作的名称。
     */
    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    // 传输服务，用于节点间的通信。
    private final TransportService transportService;

    // 命名写入注册表，用于序列化和反序列化。
    private final NamedWriteableRegistry namedWriteableRegistry;

    // 处理发布请求的函数。
    private final Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest;

    /**
     * 最后观察到的集群状态的原子引用。
     */
    private AtomicReference<ClusterState> lastSeenClusterState = new AtomicReference<>();

    /**
     * 接收到的完整集群状态计数器。
     */
    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();

    /**
     * 接收到的不兼容集群状态差异计数器。
     */
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();

    /**
     * 接收到的兼容集群状态差异计数器。
     */
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();

    /**
     * 状态请求的传输选项。
     * 这里没有设置超时，因为我们希望响应最终能被接收到，即使它在超时后到达，也不要记录错误。
     */
    private final TransportRequestOptions stateRequestOptions = TransportRequestOptions.builder()
        .withType(TransportRequestOptions.Type.STATE).build();

    public PublicationTransportHandler(TransportService transportService, NamedWriteableRegistry namedWriteableRegistry,
                                       Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
                                       BiConsumer<ApplyCommitRequest, ActionListener<Void>> handleApplyCommit) {
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handlePublishRequest = handlePublishRequest;

        transportService.registerRequestHandler(PUBLISH_STATE_ACTION_NAME, BytesTransportRequest::new, ThreadPool.Names.GENERIC,
            false, false, (request, channel, task) -> channel.sendResponse(handleIncomingPublishRequest(request)));

        transportService.registerRequestHandler(PublishClusterStateAction.SEND_ACTION_NAME, BytesTransportRequest::new,
            ThreadPool.Names.GENERIC,
            false, false, (request, channel, task) -> {
                handleIncomingPublishRequest(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

        transportService.registerRequestHandler(COMMIT_STATE_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            ApplyCommitRequest::new,
            (request, channel, task) -> handleApplyCommit.accept(request, transportCommitCallback(channel)));

        transportService.registerRequestHandler(PublishClusterStateAction.COMMIT_ACTION_NAME,
            PublishClusterStateAction.CommitClusterStateRequest::new,
            ThreadPool.Names.GENERIC, false, false,
            (request, channel, task) -> {
                final Optional<ClusterState> matchingClusterState = Optional.ofNullable(lastSeenClusterState.get()).filter(
                    cs -> cs.stateUUID().equals(request.stateUUID));
                if (matchingClusterState.isPresent() == false) {
                    throw new IllegalStateException("can't resolve cluster state with uuid" +
                        " [" + request.stateUUID + "] to commit");
                }
                final ApplyCommitRequest applyCommitRequest = new ApplyCommitRequest(matchingClusterState.get().getNodes().getMasterNode(),
                    matchingClusterState.get().term(), matchingClusterState.get().version());
                handleApplyCommit.accept(applyCommitRequest, transportCommitCallback(channel));
            });
    }

    private ActionListener<Void> transportCommitCallback(TransportChannel channel) {
        return new ActionListener<Void>() {

            @Override
            public void onResponse(Void aVoid) {
                try {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (IOException e) {
                    logger.debug("failed to send response on commit", e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (IOException ie) {
                    e.addSuppressed(ie);
                    logger.debug("failed to send response on commit", e);
                }
            }
        };
    }

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get());
    }

    public interface PublicationContext {

        void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                ActionListener<PublishWithJoinResponse> responseActionListener);

        void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommitRequest,
                             ActionListener<TransportResponse.Empty> responseActionListener);

    }

    /**
     * 创建一个新的发布上下文。
     * @param clusterChangedEvent 集群变更事件。
     * @return 返回一个新的PublicationContext实例。
     */
    public PublicationContext newPublicationContext(ClusterChangedEvent clusterChangedEvent) {
        final DiscoveryNodes nodes = clusterChangedEvent.state().nodes(); // 获取集群节点信息
        final ClusterState newState = clusterChangedEvent.state(); // 获取新状态
        final ClusterState previousState = clusterChangedEvent.previousState(); // 获取之前的状态
        final boolean sendFullVersion = clusterChangedEvent.previousState().getBlocks().disableStatePersistence(); // 是否发送完整版本
        final Map<Version, BytesReference> serializedStates = new HashMap<>(); // 序列化状态的映射
        final Map<Version, BytesReference> serializedDiffs = new HashMap<>(); // 序列化差异的映射

        // 尽早构建这些，作为在错误情况下不提交的最佳努力。
        // 遗憾的是，这并不完美，因为基于差异的发布失败可能会导致基于旧版本的完整序列化，
        // 这可能在变更已提交后失败。
        buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(),
            nodes, sendFullVersion, serializedStates, serializedDiffs);

        // 定义一个新的发布上下文实现
        return new PublicationContext() {
            // 覆盖sendPublishRequest方法以发送发布请求
            @Override
            public void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                           ActionListener<PublishWithJoinResponse> responseActionListener) {
                // 断言传入的发布请求状态与集群变更事件状态一致
                assert publishRequest.getAcceptedState() == clusterChangedEvent.state() : "state got switched on us";

                // 如果目标节点是本地节点，则在本地执行发布请求处理
                if (destination.equals(nodes.getLocalNode())) {
                    // 执行发布请求处理并响应
                    transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                        // 覆盖onFailure方法以处理可能的异常
                        @Override
                        public void onFailure(Exception e) {
                            responseActionListener.onFailure(new TransportException(e));
                        }

                        // 覆盖doRun方法以执行发布请求处理
                        @Override
                        protected void doRun() {
                            responseActionListener.onResponse(handlePublishRequest.apply(publishRequest));
                        }

                        // 覆盖toString方法以提供Runnable的字符串表示
                        @Override
                        public String toString() {
                            return "publish to self of " + publishRequest;
                        }
                    });
                } else if (sendFullVersion || !previousState.nodes().nodeExists(destination)) {
                    // 如果需要发送完整版本或目标节点在之前状态中不存在，则发送完整集群状态
                    logger.trace("sending full cluster state version {} to {}", newState.version(), destination);
                    PublicationTransportHandler.this.sendFullClusterState(newState, serializedStates, destination, responseActionListener);
                } else {
                    // 否则发送集群状态差异
                    logger.trace("sending cluster state diff for version {} to {}", newState.version(), destination);
                    PublicationTransportHandler.this.sendClusterStateDiff(newState, serializedDiffs, serializedStates, destination,
                        responseActionListener);
                }
            }

            // 覆盖sendApplyCommit方法以发送应用提交请求
            @Override
            public void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommitRequest,
                                        ActionListener<TransportResponse.Empty> responseActionListener) {
                // 根据目标节点是否是Zen1节点选择操作名称和传输请求
                final String actionName;
                final TransportRequest transportRequest;
                if (Coordinator.isZen1Node(destination)) {
                    actionName = PublishClusterStateAction.COMMIT_ACTION_NAME;
                    transportRequest = new PublishClusterStateAction.CommitClusterStateRequest(newState.stateUUID());
                } else {
                    actionName = COMMIT_STATE_ACTION_NAME;
                    transportRequest = applyCommitRequest;
                }
                // 发送应用提交请求
                transportService.sendRequest(destination, actionName, transportRequest, stateRequestOptions,
                    new TransportResponseHandler<TransportResponse.Empty>() {
                        // 覆盖read方法以读取响应
                        @Override
                        public TransportResponse.Empty read(StreamInput in) {
                            return TransportResponse.Empty.INSTANCE;
                        }

                        // 覆盖handleResponse方法以处理响应
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            responseActionListener.onResponse(response);
                        }

                        // 覆盖handleException方法以处理异常
                        @Override
                        public void handleException(TransportException exp) {
                            responseActionListener.onFailure(exp);
                        }

                        // 覆盖executor方法以指定响应处理器的线程池
                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }
                    });
            }
        };
    }

    private void sendClusterStateToNode(ClusterState clusterState, BytesReference bytes, DiscoveryNode node,
                                        ActionListener<PublishWithJoinResponse> responseActionListener, boolean sendDiffs,
                                        Map<Version, BytesReference> serializedStates) {
        try {
            final BytesTransportRequest request = new BytesTransportRequest(bytes, node.getVersion());
            final Consumer<TransportException> transportExceptionHandler = exp -> {
                if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                    logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                    sendFullClusterState(clusterState, serializedStates, node, responseActionListener);
                } else {
                    logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", node), exp);
                    responseActionListener.onFailure(exp);
                }
            };
            final TransportResponseHandler<PublishWithJoinResponse> publishWithJoinResponseHandler =
                new TransportResponseHandler<PublishWithJoinResponse>() {

                    @Override
                    public PublishWithJoinResponse read(StreamInput in) throws IOException {
                        return new PublishWithJoinResponse(in);
                    }

                    @Override
                    public void handleResponse(PublishWithJoinResponse response) {
                        responseActionListener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        transportExceptionHandler.accept(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                };
            final String actionName;
            final TransportResponseHandler<?> transportResponseHandler;
            if (Coordinator.isZen1Node(node)) {
                actionName = PublishClusterStateAction.SEND_ACTION_NAME;
                transportResponseHandler = publishWithJoinResponseHandler.wrap(empty -> new PublishWithJoinResponse(
                    new PublishResponse(clusterState.term(), clusterState.version()),
                    Optional.of(new Join(node, transportService.getLocalNode(), clusterState.term(), clusterState.term(),
                        clusterState.version()))), in -> TransportResponse.Empty.INSTANCE);
            } else {
                actionName = PUBLISH_STATE_ACTION_NAME;
                transportResponseHandler = publishWithJoinResponseHandler;
            }
            transportService.sendRequest(node, actionName, request, stateRequestOptions, transportResponseHandler);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", node), e);
            responseActionListener.onFailure(e);
        }
    }

    private static void buildDiffAndSerializeStates(ClusterState clusterState, ClusterState previousState, DiscoveryNodes discoveryNodes,
                                                    boolean sendFullVersion, Map<Version, BytesReference> serializedStates,
                                                    Map<Version, BytesReference> serializedDiffs) {
        Diff<ClusterState> diff = null;
        for (DiscoveryNode node : discoveryNodes) {
            if (node.equals(discoveryNodes.getLocalNode())) {
                // ignore, see newPublicationContext
                continue;
            }
            try {
                if (sendFullVersion || !previousState.nodes().nodeExists(node)) {
                    if (serializedStates.containsKey(node.getVersion()) == false) {
                        serializedStates.put(node.getVersion(), serializeFullClusterState(clusterState, node.getVersion()));
                    }
                } else {
                    // will send a diff
                    if (diff == null) {
                        diff = clusterState.diff(previousState);
                    }
                    if (serializedDiffs.containsKey(node.getVersion()) == false) {
                        serializedDiffs.put(node.getVersion(), serializeDiffClusterState(diff, node.getVersion()));
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, node);
            }
        }
    }

    private void sendFullClusterState(ClusterState clusterState, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        BytesReference bytes = serializedStates.get(node.getVersion());
        if (bytes == null) {
            try {
                bytes = serializeFullClusterState(clusterState, node.getVersion());
                serializedStates.put(node.getVersion(), bytes);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to serialize cluster state before publishing it to node {}", node), e);
                responseActionListener.onFailure(e);
                return;
            }
        }
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, false, serializedStates);
    }

    private void sendClusterStateDiff(ClusterState clusterState,
                                      Map<Version, BytesReference> serializedDiffs, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        final BytesReference bytes = serializedDiffs.get(node.getVersion());
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.getVersion() + "]";
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, true, serializedStates);
    }

    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        return bStream.bytes();
    }

    public static BytesReference serializeDiffClusterState(Diff diff, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(false);
            diff.writeTo(stream);
        }
        return bStream.bytes();
    }

    private PublishWithJoinResponse handleIncomingPublishRequest(BytesTransportRequest request) throws IOException {
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        try {
            if (compressor != null) {
                in = compressor.streamInput(in);
            }
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            in.setVersion(request.version());
            // If true we received full cluster state - otherwise diffs
            if (in.readBoolean()) {
                final ClusterState incomingState;
                try {
                    incomingState = ClusterState.readFrom(in, transportService.getLocalNode());
                } catch (Exception e){
                    logger.warn("unexpected error while deserializing an incoming cluster state", e);
                    throw e;
                }
                fullClusterStateReceivedCount.incrementAndGet();
                logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(),
                    request.bytes().length());
                final PublishWithJoinResponse response = handlePublishRequest.apply(new PublishRequest(incomingState));
                lastSeenClusterState.set(incomingState);
                return response;
            } else {
                final ClusterState lastSeen = lastSeenClusterState.get();
                if (lastSeen == null) {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                } else {
                    final ClusterState incomingState;
                    try {
                        Diff<ClusterState> diff = ClusterState.readDiffFrom(in, lastSeen.nodes().getLocalNode());
                        incomingState = diff.apply(lastSeen); // might throw IncompatibleClusterStateVersionException
                    } catch (IncompatibleClusterStateVersionException e) {
                        incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                        throw e;
                    } catch (Exception e){
                        logger.warn("unexpected error while deserializing an incoming cluster state", e);
                        throw e;
                    }
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                    final PublishWithJoinResponse response = handlePublishRequest.apply(new PublishRequest(incomingState));
                    lastSeenClusterState.compareAndSet(lastSeen, incomingState);
                    return response;
                }
            }
        } finally {
            IOUtils.close(in);
        }
    }
}
