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

    /**
     * PublicationTransportHandler类的构造方法，用于设置集群状态发布和提交的传输处理器。
     *
     * @param transportService 传输服务，用于节点间的消息传递。
     * @param namedWriteableRegistry 命名写入可写注册表，用于序列化和反序列化过程中的类型查找。
     * @param handlePublishRequest 处理发布请求的函数，将发布请求转换为发布响应。
     * @param handleApplyCommit 处理应用提交请求的双消费者，接受提交请求和动作监听器。
     */
    public PublicationTransportHandler(
        TransportService transportService,
        NamedWriteableRegistry namedWriteableRegistry,
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
        BiConsumer<ApplyCommitRequest, ActionListener<Void>> handleApplyCommit) {

        // 保存传入的传输服务和注册表。
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        // 保存处理发布请求的函数。
        this.handlePublishRequest = handlePublishRequest;

        // 注册处理发布集群状态请求的处理器。
        transportService.registerRequestHandler(
            PUBLISH_STATE_ACTION_NAME, BytesTransportRequest::new, ThreadPool.Names.GENERIC,
            false, false, (request, channel, task) -> channel.sendResponse(handleIncomingPublishRequest(request)));

        // 注册处理发送集群状态请求的处理器。
        transportService.registerRequestHandler(
            PublishClusterStateAction.SEND_ACTION_NAME, BytesTransportRequest::new,
            ThreadPool.Names.GENERIC,
            false, false, (request, channel, task) -> {
                handleIncomingPublishRequest(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

        // 注册处理提交集群状态请求的处理器。
        transportService.registerRequestHandler(
            COMMIT_STATE_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            ApplyCommitRequest::new,
            (request, channel, task) -> handleApplyCommit.accept(request, transportCommitCallback(channel)));

        // 注册处理提交集群状态动作的处理器。
        transportService.registerRequestHandler(
            PublishClusterStateAction.COMMIT_ACTION_NAME,
            PublishClusterStateAction.CommitClusterStateRequest::new,
            ThreadPool.Names.GENERIC, false, false,
            (request, channel, task) -> {
                // 检查是否存在匹配的集群状态。
                final Optional<ClusterState> matchingClusterState = Optional.ofNullable(lastSeenClusterState.get()).filter(
                    cs -> cs.stateUUID().equals(request.stateUUID));
                if (matchingClusterState.isPresent() == false) {
                    // 如果没有找到匹配的集群状态，抛出异常。
                    throw new IllegalStateException("can't resolve cluster state with uuid [" + request.stateUUID + "] to commit");
                }
                // 创建应用提交请求。
                final ApplyCommitRequest applyCommitRequest = new ApplyCommitRequest(
                    matchingClusterState.get().getNodes().getMasterNode(),
                    matchingClusterState.get().term(),
                    matchingClusterState.get().version());
                // 接受应用提交请求和传输提交回调。
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

    /**
     * 将集群状态发送到指定节点。
     * 根据是否发送差异或完整状态，以及序列化的状态信息，将集群状态发送到远程节点。
     *
     * @param clusterState 要发送的集群状态。
     * @param bytes 序列化后的集群状态字节。
     * @param node 目标节点。
     * @param responseActionListener 发送请求的响应监听器。
     * @param sendDiffs 是否发送集群状态差异。
     * @param serializedStates 序列化状态的映射，包含不同版本对应的序列化字节。
     */
    private void sendClusterStateToNode(ClusterState clusterState, BytesReference bytes, DiscoveryNode node,
                                        ActionListener<PublishWithJoinResponse> responseActionListener, boolean sendDiffs,
                                        Map<Version, BytesReference> serializedStates) {
        try {
            // 创建一个字节传输请求，包含序列化状态和节点版本信息。
            final BytesTransportRequest request = new BytesTransportRequest(bytes, node.getVersion());
            // 定义传输异常处理器，用于处理发送过程中的异常。
            final Consumer<TransportException> transportExceptionHandler = exp -> {
                if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                    // 如果发送差异且版本不兼容，则重新发送完整集群状态。
                    logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                    sendFullClusterState(clusterState, serializedStates, node, responseActionListener);
                } else {
                    // 其他情况下记录失败日志，并调用监听器的onFailure方法。
                    logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", node), exp);
                    responseActionListener.onFailure(exp);
                }
            };
            // 创建响应处理器，用于读取响应并调用监听器的onResponse或onFailure方法。
            final TransportResponseHandler<PublishWithJoinResponse> publishWithJoinResponseHandler =
                // 省略了TransportResponseHandler的实现细节。
                ;
            // 根据节点是否是Zen1节点，选择不同的动作名称和响应处理器。
            final String actionName;
            final TransportResponseHandler<?> transportResponseHandler;
            if (Coordinator.isZen1Node(node)) {
                actionName = PublishClusterStateAction.SEND_ACTION_NAME;
                transportResponseHandler = publishWithJoinResponseHandler.wrap(
                    // 省略了wrap方法的实现细节。
                );
            } else {
                actionName = PUBLISH_STATE_ACTION_NAME;
                transportResponseHandler = publishWithJoinResponseHandler;
            }
            // 发送请求到目标节点。
            transportService.sendRequest(node, actionName, request, stateRequestOptions, transportResponseHandler);
        } catch (Exception e) {
            // 发送过程中的异常被捕获，记录警告日志，并调用监听器的onFailure方法。
            logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", node), e);
            responseActionListener.onFailure(e);
        }
    }

    /**
     * 构建集群状态的差异并序列化状态。
     * 此方法为每个节点生成完整或差异的集群状态序列化版本，以便发布。
     *
     * @param clusterState          当前集群状态。
     * @param previousState        上一个集群状态。
     * @param discoveryNodes       发现的节点列表。
     * @param sendFullVersion      是否发送完整版本的集群状态。
     * @param serializedStates     已序列化的完整集群状态映射，按节点版本分类。
     * @param serializedDiffs      已序列化的差异集群状态映射，按节点版本分类。
     */
    private static void buildDiffAndSerializeStates(
        ClusterState clusterState, ClusterState previousState, DiscoveryNodes discoveryNodes,
        boolean sendFullVersion, Map<Version, BytesReference> serializedStates,
        Map<Version, BytesReference> serializedDiffs) {

        // 用于存储集群状态差异的变量。
        Diff<ClusterState> diff = null;
        for (DiscoveryNode node : discoveryNodes) {
            // 忽略本地节点，参考newPublicationContext方法。
            if (node.equals(discoveryNodes.getLocalNode())) {
                continue;
            }
            try {
                // 如果需要发送完整版本或目标节点在之前状态中不存在，则序列化完整状态。
                if (sendFullVersion || !previousState.nodes().nodeExists(node)) {
                    // 只有当尚未为该版本序列化时，才序列化完整集群状态。
                    if (serializedStates.containsKey(node.getVersion()) == false) {
                        serializedStates.put(node.getVersion(), serializeFullClusterState(clusterState, node.getVersion()));
                    }
                } else {
                    // 否则，将发送集群状态差异。
                    // 如果差异尚未计算，则计算差异。
                    if (diff == null) {
                        diff = clusterState.diff(previousState);
                    }
                    // 只有当尚未为该版本序列化时，才序列化差异集群状态。
                    if (serializedDiffs.containsKey(node.getVersion()) == false) {
                        serializedDiffs.put(node.getVersion(), serializeDiffClusterState(diff, node.getVersion()));
                    }
                }
            } catch (IOException e) {
                // 如果序列化失败，抛出异常。
                throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, node);
            }
        }
    }

    /**
     * 向指定节点发送完整集群状态。
     * 此方法负责序列化集群状态并发送给远程节点。
     *
     * @param clusterState          要发送的集群状态。
     * @param serializedStates      已序列化的完整集群状态映射，按节点版本分类。
     * @param node                  目标节点。
     * @param responseActionListener 发送请求的响应监听器。
     */
    private void sendFullClusterState(ClusterState clusterState, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        // 获取对应版本节点的序列化状态。
        BytesReference bytes = serializedStates.get(node.getVersion());
        if (bytes == null) {
            try {
                // 如果尚未序列化，则进行序列化。
                bytes = serializeFullClusterState(clusterState, node.getVersion());
                // 存储序列化状态以备将来使用。
                serializedStates.put(node.getVersion(), bytes);
            } catch (Exception e) {
                // 序列化过程中的异常被捕获，记录警告日志，并通知响应监听器。
                logger.warn(() -> new ParameterizedMessage("failed to serialize cluster state before publishing it to node {}", node), e);
                responseActionListener.onFailure(e);
                return;
            }
        }
        // 发送序列化状态到节点，指定不发送差异。
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, false, serializedStates);
    }

    /**
     * 向指定节点发送集群状态差异。
     * 此方法使用已序列化的差异状态进行发送。
     *
     * @param clusterState          要发送的集群状态。
     * @param serializedDiffs       已序列化的差异集群状态映射，按节点版本分类。
     * @param serializedStates      已序列化的完整集群状态映射，按节点版本分类。
     * @param node                  目标节点。
     * @param responseActionListener 发送请求的响应监听器。
     */
    private void sendClusterStateDiff(ClusterState clusterState,
                                      Map<Version, BytesReference> serializedDiffs, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        // 获取对应版本节点的序列化差异状态。
        final BytesReference bytes = serializedDiffs.get(node.getVersion());
        // 断言确保已序列化的差异状态不为空。
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.getVersion() + "]";
        // 发送序列化差异状态到节点，指定发送差异。
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, true, serializedStates);
    }

    /**
     * 序列化完整集群状态。
     * 此方法将集群状态序列化为字节流，以便网络传输。
     *
     * @param clusterState 要序列化的集群状态。
     * @param nodeVersion 目标节点的版本。
     * @return 序列化后的集群状态字节引用。
     * @throws IOException 序列化过程中发生的IO异常。
     */
    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(true); // 标记为完整状态。
            clusterState.writeTo(stream);
        }
        return bStream.bytes();
    }

    /**
     * 序列化集群状态差异。
     * 此方法将集群状态差异序列化为字节流，以便网络传输。
     *
     * @param diff 集群状态差异。
     * @param nodeVersion 目标节点的版本。
     * @return 序列化后的差异状态字节引用。
     * @throws IOException 序列化过程中发生的IO异常。
     */
    public static BytesReference serializeDiffClusterState(Diff diff, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(false); // 标记为差异状态。
            diff.writeTo(stream);
        }
        return bStream.bytes();
    }

    /**
     * 处理接收到的集群状态发布请求。
     * 此方法解析请求中的序列化集群状态数据，根据是完整状态还是差异状态来更新本地集群状态。
     *
     * @param request 包含序列化集群状态数据的字节传输请求。
     * @return 返回发布与加入响应。
     * @throws IOException 处理请求过程中发生的IO异常。
     */
    private PublishWithJoinResponse handleIncomingPublishRequest(BytesTransportRequest request) throws IOException {
        // 获取请求字节数据的压缩器。
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        try {
            // 如果存在压缩器，则使用它来包装输入流。
            if (compressor != null) {
                in = compressor.streamInput(in);
            }
            // 创建命名写入感知输入流。
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            // 设置输入流的版本信息。
            in.setVersion(request.version());
            // 读取布尔值以确定接收到的是完整集群状态还是差异状态。
            if (in.readBoolean()) {
                // 处理完整集群状态。
                final ClusterState incomingState;
                try {
                    incomingState = ClusterState.readFrom(in, transportService.getLocalNode());
                } catch (Exception e) {
                    logger.warn("unexpected error while deserializing an incoming cluster state", e);
                    throw e;
                }
                // 增加完整集群状态接收计数。
                fullClusterStateReceivedCount.incrementAndGet();
                // 记录接收到的完整集群状态信息。
                logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(),
                    request.bytes().length());
                // 处理发布请求并返回响应。
                final PublishWithJoinResponse response = handlePublishRequest.apply(new PublishRequest(incomingState));
                // 更新最近观察到的集群状态。
                lastSeenClusterState.set(incomingState);
                return response;
            } else {
                // 处理集群状态差异。
                final ClusterState lastSeen = lastSeenClusterState.get();
                if (lastSeen == null) {
                    // 如果本地没有集群状态，无法应用差异。
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    // 增加不兼容集群状态差异接收计数。
                    incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                } else {
                    final ClusterState incomingState;
                    try {
                        // 读取集群状态差异并应用到最近观察到的集群状态。
                        Diff<ClusterState> diff = ClusterState.readDiffFrom(in, lastSeen.nodes().getLocalNode());
                        incomingState = diff.apply(lastSeen);
                    } catch (IncompatibleClusterStateVersionException e) {
                        // 增加不兼容集群状态差异接收计数。
                        incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                        throw e;
                    } catch (Exception e) {
                        logger.warn("unexpected error while deserializing an incoming cluster state", e);
                        throw e;
                    }
                    // 增加兼容集群状态差异接收计数。
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    // 记录接收到的差异集群状态信息。
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                    // 处理发布请求并返回响应。
                    final PublishWithJoinResponse response = handlePublishRequest.apply(new PublishRequest(incomingState));
                    // 更新最近观察到的集群状态。
                    lastSeenClusterState.compareAndSet(lastSeen, incomingState);
                    return response;
                }
            }
        } finally {
            // 最后，关闭输入流以释放资源。
            IOUtils.close(in);
        }
    }
}
