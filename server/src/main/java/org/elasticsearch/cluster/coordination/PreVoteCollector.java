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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.CoordinationState.isElectionQuorum;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

public class PreVoteCollector {
    // 日志记录器
    private static final Logger logger = LogManager.getLogger(PreVoteCollector.class);

    // 请求预投票操作的名称
    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    // 传输服务，用于节点间的通信
    private final TransportService transportService;
    // 开始选举的动作
    private final Runnable startElection;
    // 更新已看到的最大任期的消费者
    private final LongConsumer updateMaxTermSeen;

    // 用于简单原子更新的元组。在第一次调用`update()`之前为null。
    // 如果目前没有已知的领导者，则DiscoveryNode组件为null。
    private volatile Tuple<DiscoveryNode, PreVoteResponse> state;


    PreVoteCollector(final TransportService transportService, final Runnable startElection, final LongConsumer updateMaxTermSeen) {
        this.transportService = transportService;
        this.startElection = startElection;
        this.updateMaxTermSeen = updateMaxTermSeen;

        // TODO does this need to be on the generic threadpool or can it use SAME?
        transportService.registerRequestHandler(REQUEST_PRE_VOTE_ACTION_NAME, Names.GENERIC, false, false,
            PreVoteRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePreVoteRequest(request)));
    }

    /**
     * 开始一个新的预投票轮次。
     *
     * @param clusterState   最后被接受的集群状态。
     * @param broadcastNodes 需要从这些节点请求预投票的节点集合。
     * @return 返回一个可关闭的预投票轮次对象，可以提前结束轮次。
     */
    public Releasable start(final ClusterState clusterState, final Iterable<DiscoveryNode> broadcastNodes) {
        // 创建一个新的PreVotingRound对象，初始化当前任期
        PreVotingRound preVotingRound = new PreVotingRound(clusterState, state.v2().getCurrentTerm());
        // 启动预投票过程，向指定的节点集合请求预投票
        preVotingRound.start(broadcastNodes);
        // 返回预投票轮次对象
        return preVotingRound;
    }

    /**
     * 获取当前的预投票响应。
     * @return 当前的预投票响应对象。
     */
    PreVoteResponse getPreVoteResponse() {
        return state.v2();
    }

    /**
     * 获取当前已知的领导者节点。
     * @return 当前已知的领导者节点，如果没有则返回null。
     */
    @Nullable
    DiscoveryNode getLeader() {
        return state.v1();
    }

    /**
     * 更新预投票响应和领导者状态。
     * @param preVoteResponse 要更新的预投票响应对象。
     * @param leader 要更新的领导者节点，如果没有则传入null。
     */
    public void update(final PreVoteResponse preVoteResponse, @Nullable final DiscoveryNode leader) {
        // 记录更新操作的日志
        logger.trace("updating with preVoteResponse={}, leader={}", preVoteResponse, leader);
        // 用新的领导者和预投票响应更新状态
        state = new Tuple<>(leader, preVoteResponse);
    }

    /**
     * 处理预投票请求。
     * 当收到其他节点发来的预投票请求时，此方法将被调用。
     *
     * @param request 预投票请求对象，包含了请求的详细信息。
     * @return 返回一个预投票响应对象，表示对请求的处理结果。
     */
    private PreVoteResponse handlePreVoteRequest(final PreVoteRequest request) {
        // 更新已看到的最大任期
        updateMaxTermSeen.accept(request.getCurrentTerm());

        // 获取当前状态
        Tuple<DiscoveryNode, PreVoteResponse> state = this.state;
        // 断言状态不为空，确保在处理预投票请求之前已经初始化
        assert state != null : "received pre-vote request before fully initialised";

        // 获取当前的领导者节点和预投票响应
        final DiscoveryNode leader = state.v1();
        final PreVoteResponse response = state.v2();

        // 如果当前没有领导者，即集群处于无领导状态，则返回当前的预投票响应
        if (leader == null) {
            return response;
        }

        // 如果请求来自当前的领导者节点
        if (leader.equals(request.getSourceNode())) {
            // 这是一个罕见的情况，我们的领导者检测到了故障并主动下台，但我们仍然是一个追随者。
            // 这是可能发生的，领导者可能失去了它的法定人数，但只要我们仍然是追随者，我们就不会向任何其他节点提供加入的邀请，
            // 因此，向我们的旧领导者提供加入没有太大的不利之处。
            // 这样做的好处是，它使得领导者不太可能改变，并且它的重新选举将比等待法定人数的追随者也检测到它的故障要快。
            return response;
        }

        // 如果有其他节点尝试成为领导者，但已经有领导者存在，则抛出异常，拒绝这个请求
        throw new CoordinationStateRejectedException("rejecting " + request + " as there is already a leader");
    }

    @Override
    public String toString() {
        return "PreVoteCollector{" +
            "state=" + state +
            '}';
    }

    private class PreVotingRound implements Releasable {
        // 已收到的预投票集合
        private final Set<DiscoveryNode> preVotesReceived = newConcurrentSet();
        // 选举是否已经开始的标记
        private final AtomicBoolean electionStarted = new AtomicBoolean();
        // 预投票请求对象
        private final PreVoteRequest preVoteRequest;
        // 当前集群状态
        private final ClusterState clusterState;
        // 是否已经关闭的标记
        private final AtomicBoolean isClosed = new AtomicBoolean();

        /**
         * 构造函数。
         * @param clusterState 当前集群状态。
         * @param currentTerm  当前任期。
         */
        PreVotingRound(final ClusterState clusterState, final long currentTerm) {
            this.clusterState = clusterState;
            preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), currentTerm);
        }
        /**
         * 开始向指定节点请求预投票。
         * @param broadcastNodes 需要请求预投票的节点集合。
         */
        void start(final Iterable<DiscoveryNode> broadcastNodes) {
            assert StreamSupport.stream(broadcastNodes.spliterator(), false).noneMatch(Coordinator::isZen1Node) : broadcastNodes;
            logger.debug("{} requesting pre-votes from {}", this, broadcastNodes);
            // 遍历节点集合，向每个节点发送预投票请求
            broadcastNodes.forEach(n -> transportService.sendRequest(n, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
                new TransportResponseHandler<PreVoteResponse>() {
                    @Override
                    public PreVoteResponse read(StreamInput in) throws IOException {
                        return new PreVoteResponse(in);
                    }

                    @Override
                    public void handleResponse(PreVoteResponse response) {
                        handlePreVoteResponse(response, n);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.debug(new ParameterizedMessage("{} failed", this), exp);
                    }

                    @Override
                    public String executor() {
                        return Names.GENERIC;
                    }

                    @Override
                    public String toString() {
                        return "TransportResponseHandler{" + PreVoteCollector.this + ", node=" + n + '}';
                    }
                }));
        }

        /**
         * 处理来自特定节点的预投票响应。
         * @param response 预投票响应。
         * @param sender   发送预投票响应的节点。
         */
        private void handlePreVoteResponse(final PreVoteResponse response, final DiscoveryNode sender) {
            // 如果已经关闭，则忽略响应
            if (isClosed.get()) {
                logger.debug("{} is closed, ignoring {} from {}", this, response, sender);
                return;
            }

            // 更新已看到的最大任期
            updateMaxTermSeen.accept(response.getCurrentTerm());

            // 如果响应中的任期或版本比当前集群状态新，则忽略该响应
            if (response.getLastAcceptedTerm() > clusterState.term()
                || (response.getLastAcceptedTerm() == clusterState.term()
                && response.getLastAcceptedVersion() > clusterState.getVersionOrMetaDataVersion())) {
                logger.debug("{} ignoring {} from {} as it is fresher", this, response, sender);
                return;
            }

            // 添加预投票
            preVotesReceived.add(sender);
            // 创建投票集合并添加所有收到的预投票
            final VoteCollection voteCollection = new VoteCollection();
            preVotesReceived.forEach(voteCollection::addVote);

            // 检查是否达到法定人数
            if (isElectionQuorum(voteCollection, clusterState) == false) {
                logger.debug("{} added {} from {}, no quorum yet", this, response, sender);
                return;
            }

            // 如果选举尚未开始，则开始选举
            if (electionStarted.compareAndSet(false, true) == false) {
                logger.debug("{} added {} from {} but election has already started", this, response, sender);
                return;
            }

            // 开始选举
            logger.debug("{} added {} from {}, starting election", this, response, sender);
            startElection.run();
        }

        // 返回类的字符串表示
        @Override
        public String toString() {
            return "PreVotingRound{" +
                "preVotesReceived=" + preVotesReceived +
                ", electionStarted=" + electionStarted +
                ", preVoteRequest=" + preVoteRequest +
                ", isClosed=" + isClosed +
                '}';
        }

        // 关闭预投票轮次
        @Override
        public void close() {
            // 确保不会重复关闭
            final boolean isNotAlreadyClosed = isClosed.compareAndSet(false, true);
            assert isNotAlreadyClosed;
        }
    }
}
