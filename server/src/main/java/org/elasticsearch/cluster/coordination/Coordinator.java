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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.FollowersChecker.FollowerCheckRequest;
import org.elasticsearch.cluster.coordination.JoinHelper.InitialJoinAccumulator;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.discovery.*;
import org.elasticsearch.discovery.zen.PendingClusterStateStats;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ID;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class Coordinator extends AbstractLifecycleComponent implements Discovery {

    // Elasticsearch 6.x版本之前使用的Zen发现算法的初始任期值
    public static final long ZEN1_BWC_TERM = 0;

    // 日志记录器，用于记录Coordinator类的日志信息
    private static final Logger logger = LogManager.getLogger(Coordinator.class);

    // 集群状态发布操作的超时设置
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.publish.timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    // 当前节点的设置
    private final Settings settings;
    // 是否使用单节点发现模式
    private final boolean singleNodeDiscovery;
    // 传输服务，用于节点间的通信
    private final TransportService transportService;
    // 主节点服务，用于管理主节点的生命周期和选举
    private final MasterService masterService;
    // 资源分配服务，用于管理集群中资源的分配
    private final AllocationService allocationService;
    // 节点加入帮助类，用于处理节点加入集群的逻辑
    private final JoinHelper joinHelper;
    // 节点移除执行器，用于处理从集群状态中移除节点的任务
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    // 持久化协调状态供应者，用于获取持久化的协调状态
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;
    // 无主节点阻塞服务，用于在没有主节点时阻塞某些操作
    private final NoMasterBlockService noMasterBlockService;

    // 互斥锁，用于同步对共享资源的访问
    final Object mutex = new Object();
    // 集群协调状态，初始化在启动时
    private final SetOnce<CoordinationState> coordinationState = new SetOnce<>();
    // 应用者状态，暴露给集群状态应用者的当前状态
    private volatile ClusterState applierState;

    // 对等发现器，用于发现集群中的其他节点
    private final PeerFinder peerFinder;
    // 预投票收集器，用于在选举过程中收集预投票
    private final PreVoteCollector preVoteCollector;
    // 随机数生成器，用于选举过程中的随机延迟等
    private final Random random;
    // 选举调度工厂，用于创建选举调度器
    private final ElectionSchedulerFactory electionSchedulerFactory;
    // 配置的种子主机解析器，用于解析配置的种子主机地址
    private final SeedHostsResolver configuredHostsResolver;
    // 发布超时时间
    private final TimeValue publishTimeout;
    // 状态发布传输处理器，用于处理集群状态的发布
    private final PublicationTransportHandler publicationHandler;
    // 主节点检查器，用于检查主节点的健康状态
    private final LeaderChecker leaderChecker;
    // 跟随者检查器，用于检查跟随者节点的状态
    private final FollowersChecker followersChecker;
    // 集群应用者，用于应用集群状态变更
    private final ClusterApplier clusterApplier;
    // 节点加入验证器集合，用于在节点加入时进行验证
    private final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators;

    // 选举调度器，用于控制选举过程的计时
    @Nullable
    private Releasable electionScheduler;
    // 预投票阶段的资源释放器
    @Nullable
    private Releasable prevotingRound;
    // 已看到的最大的任期值
    private long maxTermSeen;
    // 重新配置器，用于处理集群配置的变更
    private final Reconfigurator reconfigurator;
    // 集群引导服务，用于处理集群启动和引导逻辑
    private final ClusterBootstrapService clusterBootstrapService;
    // 发现升级服务，用于处理集群升级过程中的发现逻辑
    private final DiscoveryUpgradeService discoveryUpgradeService;
    // 延迟检测器，用于检测和处理节点的延迟问题
    private final LagDetector lagDetector;
    // 集群形成失败帮助器，用于处理集群无法形成时的逻辑
    private final ClusterFormationFailureHelper clusterFormationFailureHelper;

    // 当前模式，如跟随者、候选者或领导者
    private Mode mode;
    // 上一个已知的主节点
    private Optional<DiscoveryNode> lastKnownLeader;
    // 上一次加入信息
    private Optional<Join> lastJoin;
    // 节点加入累加器，用于跟踪节点加入请求
    private JoinHelper.JoinAccumulator joinAccumulator;
    // 当前正在进行的发布操作
    private Optional<CoordinatorPublication> currentPublication = Optional.empty();

    public Coordinator(String nodeName, Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                       NamedWriteableRegistry namedWriteableRegistry, AllocationService allocationService, MasterService masterService,
                       Supplier<CoordinationState.PersistedState> persistedStateSupplier, SeedHostsProvider seedHostsProvider,
                       ClusterApplier clusterApplier,
                       Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators, Random random) {
        // 节点名称
        this.settings = settings;
        // 传输服务，用于节点间的通信
        this.transportService = transportService;
        // 主服务，处理主节点相关逻辑
        this.masterService = masterService;
        // 资源分配服务，处理集群中资源的分配
        this.allocationService = allocationService;
        // 注册加入验证器，包括内置和自定义验证器
        this.onJoinValidators = JoinTaskExecutor.addBuiltInJoinValidators(onJoinValidators);
        // 是否启用单节点发现模式
        this.singleNodeDiscovery = DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE.equals(
            DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings));
        // 初始化节点加入帮助类
        this.joinHelper = new JoinHelper(settings, allocationService, masterService, transportService,
            this::getCurrentTerm, this::getStateForMasterService, this::handleJoinRequest, this::joinLeaderInTerm, this.onJoinValidators);
        // 持久化状态供应器
        this.persistedStateSupplier = persistedStateSupplier;
        // 无主节点阻塞服务
        this.noMasterBlockService = new NoMasterBlockService(settings, clusterSettings);
        // 上一个已知的主节点
        this.lastKnownLeader = Optional.empty();
        // 上一次加入信息
        this.lastJoin = Optional.empty();
        // 节点加入累加器
        this.joinAccumulator = new InitialJoinAccumulator();
        // 集群状态发布超时设置
        this.publishTimeout = PUBLISH_TIMEOUT_SETTING.get(settings);
        // 随机数生成器
        this.random = random;
        // 选举调度工厂
        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, random, transportService.getThreadPool());
        // 预投票收集器
        this.preVoteCollector = new PreVoteCollector(transportService, this::startElection, this::updateMaxTermSeen);
        // 配置的种子主机解析器
        this.configuredHostsResolver = new SeedHostsResolver(nodeName, settings, transportService, seedHostsProvider);
        // 对等发现器
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService,
            new HandshakingTransportAddressConnector(settings, transportService), configuredHostsResolver);
        // 状态发布传输处理器
        this.publicationHandler = new PublicationTransportHandler(transportService, namedWriteableRegistry,
            this::handlePublishRequest, this::handleApplyCommit);
        // 主节点检查器
        this.leaderChecker = new LeaderChecker(settings, transportService, getOnLeaderFailure());
        // 跟随者节点检查器
        this.followersChecker = new FollowersChecker(settings, transportService, this::onFollowerCheckRequest, this::removeNode);
        // 节点移除执行器
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        // 集群应用者
        this.clusterApplier = clusterApplier;
        // 设置集群状态供应器
        masterService.setClusterStateSupplier(this::getStateForMasterService);
        // 重新配置器
        this.reconfigurator = new Reconfigurator(settings, clusterSettings);
        // 集群引导服务
        this.clusterBootstrapService = new ClusterBootstrapService(settings, transportService, this::getFoundPeers,
            this::isInitialConfigurationSet, this::setInitialConfiguration);
        // 发现升级服务
        this.discoveryUpgradeService = new DiscoveryUpgradeService(settings, transportService,
            this::isInitialConfigurationSet, joinHelper, peerFinder::getFoundPeers, this::setInitialConfiguration);
        // 延迟检测器
        this.lagDetector = new LagDetector(settings, transportService.getThreadPool(), n -> removeNode(n, "lagging"),
            transportService::getLocalNode);
        // 集群形成失败帮助器
        this.clusterFormationFailureHelper = new ClusterFormationFailureHelper(settings, this::getClusterFormationState,
            transportService.getThreadPool(), joinHelper::logLastFailedJoinAttempt);
    }

    /**
     * 获取集群形成状态。
     * <p>
     * 这个方法创建并返回一个包含当前集群形成状态的ClusterFormationState对象。
     * 它包括设置、主节点服务获取的集群状态、最近解析的地址、已发现的节点列表和当前任期。
     *
     * @return 集群形成状态对象
     */
    private ClusterFormationState getClusterFormationState() {
        return new ClusterFormationState(settings, getStateForMasterService(), peerFinder.getLastResolvedAddresses(),
            StreamSupport.stream(peerFinder.getFoundPeers().spliterator(), false).collect(Collectors.toList()), getCurrentTerm());
    }

    /**
     * 获取主节点失败时的回调任务。
     * <p>
     * 这个方法返回一个Runnable对象，当主节点失败时会被执行。
     * 它将当前节点状态设置为候选人（Candidate）状态，准备进行新的选举。
     *
     * @return 主节点失败时执行的Runnable对象
     */
    private Runnable getOnLeaderFailure() {
        return new Runnable() {
            @Override
            public void run() {
                synchronized (mutex) {
                    // 当主节点失败时，变成候选人准备新的选举
                    becomeCandidate("onLeaderFailure");
                }
            }

            @Override
            public String toString() {
                // 返回这个Runnable对象的描述
                return "notification of leader failure";
            }
        };
    }

    /**
     * 移除节点。
     * <p>
     * 这个方法从集群状态中移除指定的节点。
     * 它在领导者模式下，通过主节点服务提交一个状态更新任务来执行节点的移除。
     *
     * @param discoveryNode 要移除的节点
     * @param reason 移除节点的原因
     */
    private void removeNode(DiscoveryNode discoveryNode, String reason) {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                // 如果当前是领导者，提交状态更新任务以移除节点
                masterService.submitStateUpdateTask("node-left",
                    new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, reason),
                    ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                    nodeRemovalExecutor,
                    nodeRemovalExecutor);
            }
        }
    }

    /**
     * 处理来自跟随者节点的检查请求。
     * <p>
     * 当一个跟随者节点发送FollowerCheckRequest来检查当前节点的状态时，这个方法会被调用。
     * 它用于确保当前节点的状态与跟随者节点的期望保持一致。
     *
     * @param followerCheckRequest 跟随者节点发送的检查请求
     */
    void onFollowerCheckRequest(FollowerCheckRequest followerCheckRequest) {
        synchronized (mutex) {
            // 确保当前任期至少与请求的任期一致
            ensureTermAtLeast(followerCheckRequest.getSender(), followerCheckRequest.getTerm());

            // 如果当前任期与请求的任期不一致，则拒绝请求
            if (getCurrentTerm() != followerCheckRequest.getTerm()) {
                logger.trace("onFollowerCheckRequest: current term is [{}], rejecting {}", getCurrentTerm(), followerCheckRequest);
                throw new CoordinationStateRejectedException("onFollowerCheckRequest: current term is ["
                    + getCurrentTerm() + "], rejecting " + followerCheckRequest);
            }

            // 检查节点是否已经接受了当前任期的状态。如果没有，说明节点在当前任期中从未提交过集群状态，
            // 因此也从未移除过NO_MASTER_BLOCK。这个逻辑确保了我们即使在接收到第一个集群状态更新之前，
            // 也能迅速将节点转换为跟随者状态，同时不必处理在将候选人转换回跟随者时可能需要从applierState中移除NO_MASTER_BLOCK的情况。
            if (getLastAcceptedState().term() < getCurrentTerm()) {
                becomeFollower("onFollowerCheckRequest", followerCheckRequest.getSender());
            } else if (mode == Mode.FOLLOWER) {
                // 如果已经是跟随者模式，则成功响应检查请求
                logger.trace("onFollowerCheckRequest: responding successfully to {}", followerCheckRequest);
            } else if (joinHelper.isJoinPending()) {
                // 如果有加入请求待处理，重新加入主节点，并成功响应检查请求
                logger.trace("onFollowerCheckRequest: rejoining master, responding successfully to {}", followerCheckRequest);
            } else {
                // 如果收到的检查请求来自一个有问题的主节点，则拒绝请求
                logger.trace("onFollowerCheckRequest: received check from faulty master, rejecting {}", followerCheckRequest);
                throw new CoordinationStateRejectedException(
                    "onFollowerCheckRequest: received check from faulty master, rejecting " + followerCheckRequest);
            }
        }
    }

    /**
     * 处理提交应用请求。
     * <p>
     * 当集群中的一个节点接收到来自主节点的ApplyCommitRequest时，这个方法会被调用。
     * 它负责更新集群状态并将更改应用到集群状态应用者（ClusterApplier）。
     *
     * @param applyCommitRequest 提交应用请求对象，包含要应用的集群状态信息
     * @param applyListener 应用结果的监听器，用于接收操作成功或失败的回调
     */
    private void handleApplyCommit(ApplyCommitRequest applyCommitRequest, ActionListener<Void> applyListener) {
        synchronized (mutex) {
            logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

            // 获取当前协调状态并处理提交请求
            coordinationState.get().handleCommit(applyCommitRequest);
            // 获取最后一次成功提交的集群状态，并根据集群是否已恢复隐藏相应状态
            final ClusterState committedState = hideStateIfNotRecovered(coordinationState.get().getLastAcceptedState());
            // 更新应用者状态，如果是候选人模式，则添加无主节点阻塞
            applierState = mode == Mode.CANDIDATE ? clusterStateWithNoMasterBlock(committedState) : committedState;
            if (applyCommitRequest.getSourceNode().equals(getLocalNode())) {
                // 如果请求来自本地主节点，则在发布过程的最后应用提交状态，而不是在这里
                applyListener.onResponse(null);
            } else {
                // 如果请求来自其他节点，则通过集群应用者应用新状态
                clusterApplier.onNewClusterState(applyCommitRequest.toString(), () -> applierState,
                    new ClusterApplyListener() {
                        @Override
                        public void onFailure(String source, Exception e) {
                            // 如果应用失败，通知监听器
                            applyListener.onFailure(e);
                        }

                        @Override
                        public void onSuccess(String source) {
                            // 如果应用成功，通知监听器
                            applyListener.onResponse(null);
                        }
                    });
            }
        }
    }

    /**
     * 处理发布请求。
     * <p>
     * 当节点接收到来自其他节点的发布请求时，这个方法会被调用。它负责验证请求的有效性，
     * 更新集群状态，并决定是否需要加入其他节点的集群。
     *
     * @param publishRequest 包含要发布的集群状态的发布请求
     * @return 包含发布结果和可能的加入信息的响应
     */
    PublishWithJoinResponse handlePublishRequest(PublishRequest publishRequest) {
        // 断言请求中接受的集群状态的本地节点与当前节点相等
        assert publishRequest.getAcceptedState().nodes().getLocalNode().equals(getLocalNode()) :
            publishRequest.getAcceptedState().nodes().getLocalNode() + " != " + getLocalNode();

        synchronized (mutex) {
            // 获取发布请求中源节点，即发送发布请求的主节点
            final DiscoveryNode sourceNode = publishRequest.getAcceptedState().nodes().getMasterNode();
            // 记录日志，说明正在处理来自特定节点的发布请求
            logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);

            // 如果请求来自本地节点，但当前模式不是领导者，则抛出异常
            if (sourceNode.equals(getLocalNode()) && mode != Mode.LEADER) {
                throw new CoordinationStateRejectedException("no longer leading this publication's term: " + publishRequest);
            }

            // 如果当前节点不是跟随者lastKnownLeader，且集群状态来自非预期的主节点，则拒绝请求
            if (publishRequest.getAcceptedState().term() == ZEN1_BWC_TERM && getCurrentTerm() == ZEN1_BWC_TERM
                && mode == Mode.FOLLOWER && Optional.of(sourceNode).equals(lastKnownLeader) == false) {
                logger.debug("received cluster state from {} but currently following {}, rejecting", sourceNode, lastKnownLeader);
                throw new CoordinationStateRejectedException("received cluster state from " + sourceNode + " but currently following "
                    + lastKnownLeader + ", rejecting");
            }

            // 如果本地集群状态的元数据已提交，但集群UUID与请求中的集群UUID不匹配，则拒绝请求
            final ClusterState localState = coordinationState.get().getLastAcceptedState();
            if (localState.metaData().clusterUUIDCommitted() &&
                localState.metaData().clusterUUID().equals(publishRequest.getAcceptedState().metaData().clusterUUID()) == false) {
                logger.warn("received cluster state from {} with a different cluster uuid {} than local cluster uuid {}, rejecting",
                    sourceNode, publishRequest.getAcceptedState().metaData().clusterUUID(), localState.metaData().clusterUUID());
                throw new CoordinationStateRejectedException("received cluster state from " + sourceNode +
                    " with a different cluster uuid " + publishRequest.getAcceptedState().metaData().clusterUUID() +
                    " than local cluster uuid " + localState.metaData().clusterUUID() + ", rejecting");
            }

            // 如果请求的集群状态任期大于本地状态任期，则执行加入验证
            if (publishRequest.getAcceptedState().term() > localState.term()) {
                onJoinValidators.forEach(a -> a.accept(getLocalNode(), publishRequest.getAcceptedState()));
            }

            // 确保当前任期至少与发布请求中的任期一致
            ensureTermAtLeast(sourceNode, publishRequest.getAcceptedState().term());
            // 处理发布请求并获取发布响应
            final PublishResponse publishResponse = coordinationState.get().handlePublishRequest(publishRequest);

            // 如果请求来自本地节点，则更新预投票收集器；否则，将当前节点设置为跟随者
            if (sourceNode.equals(getLocalNode())) {
                preVoteCollector.update(getPreVoteResponse(), getLocalNode());
            } else {
                becomeFollower("handlePublishRequest", sourceNode); // also updates preVoteCollector
            }

            // 创建并返回包含发布响应和可能的加入信息的响应对象
            return new PublishWithJoinResponse(publishResponse,
                joinWithDestination(lastJoin, sourceNode, publishRequest.getAcceptedState().term()));
        }
    }

    /**
     * 根据目标和任期检查是否可以重新使用上一次的加入请求。
     * <p>
     * 如果上一次的加入请求存在，并且目标节点与当前领导者匹配，同时任期也相同，
     * 则返回上一次的加入请求；否则返回空。
     *
     * @param lastJoin 上一次的加入请求
     * @param leader 当前的领导者节点
     * @param term 当前任期
     * @return 如果条件满足，返回上一次的加入请求；否则返回空
     */
    private static Optional<Join> joinWithDestination(Optional<Join> lastJoin, DiscoveryNode leader, long term) {
        // 检查上一次的加入请求是否存在，目标节点是否匹配，任期是否相同
        if (lastJoin.isPresent()
            && lastJoin.get().targetMatches(leader)
            && lastJoin.get().getTerm() == term) {
            return lastJoin;
        }
        // 如果不满足条件，返回空
        return Optional.empty();
    }

    /**
     * 关闭预投票和选举调度器。
     * <p>
     * 这个方法用于关闭当前的预投票轮次和选举调度器，释放相关资源。
     * 通常在选举过程结束或需要清理资源时调用。
     */
    private void closePrevotingAndElectionScheduler() {
        // 如果存在预投票轮次，关闭并清理
        if (prevotingRound != null) {
            prevotingRound.close();
            prevotingRound = null;
        }

        // 如果存在选举调度器，关闭并清理
        if (electionScheduler != null) {
            electionScheduler.close();
            electionScheduler = null;
        }
    }


    /**
     * 更新节点所看到的任期的最大值。
     * 如果当前任期小于所看到的最大任期，则根据当前模式采取不同的行动。
     *
     * @param term 观察到的新的最大任期值。
     */
    private void updateMaxTermSeen(final long term) {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 更新最大任期值，取当前的最大任期和传入任期中的较大者。
            maxTermSeen = Math.max(maxTermSeen, term);

            // 获取当前的任期值。
            final long currentTerm = getCurrentTerm();

            // 如果当前模式是领导者，并且观察到的最大任期大于当前任期，则需要进一步处理。
            if (mode == Mode.LEADER && maxTermSeen > currentTerm) {
                // 如果有发布操作正在进行中，则不提升任期，因为提升任期会取消正在进行的发布操作。
                // 我们也会在发布操作结束时检查是否需要提升任期。
                if (publicationInProgress()) {
                    logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, enqueueing term bump", maxTermSeen, currentTerm);
                } else {
                    try {
                        // 记录日志，准备提升任期。
                        logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, bumping term", maxTermSeen, currentTerm);

                        // 确保本地节点的任期至少提升到观察到的最大任期。
                        ensureTermAtLeast(getLocalNode(), maxTermSeen);

                        // 开始选举过程。
                        startElection();
                    } catch (Exception e) {
                        // 如果在提升任期或开始选举过程中出现异常，记录警告日志，并变为候选人模式。
                        logger.warn(new ParameterizedMessage("failed to bump term to {}", maxTermSeen), e);
                        becomeCandidate("updateMaxTermSeen");
                    }
                }
            }
        }
    }

    /**
     * 启动选举过程。
     * 在此方法中，如果当前节点是候选人模式，并且本地节点是选举法定人数的一部分，则向所有已知节点发送开始加入请求。
     */
    private void startElection() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 检查当前模式是否为候选人，因为预投票收集器仅在候选人模式下活跃。
            // 由于预投票收集器调用此方法时不进行同步，因此我们需要在这里再次检查模式。
            if (mode == Mode.CANDIDATE) {
                // 如果本地节点不是选举法定人数的一部分，则跳过选举。
                if (electionQuorumContainsLocalNode(getLastAcceptedState()) == false) {
                    logger.trace("skip election as local node is not part of election quorum: {}",
                        getLastAcceptedState().coordinationMetaData());
                    return;
                }

                // 创建一个新的开始加入请求，其任期为当前任期和观察到的最大任期中的较大者加1。
                final StartJoinRequest startJoinRequest
                    = new StartJoinRequest(getLocalNode(), Math.max(getCurrentTerm(), maxTermSeen) + 1);

                // 记录开始选举的日志。
                logger.debug("starting election with {}", startJoinRequest);

                // 遍历所有已发现的节点。
                getDiscoveredNodes().forEach(node -> {
                    // 如果节点不是Zen1节点，则向该节点发送开始加入请求。
                    if (isZen1Node(node) == false) {
                        joinHelper.sendStartJoinRequest(startJoinRequest, node);
                    }
                });
            }
        }
    }

    /**
     * 退位给指定的新主节点。
     * 此方法将当前节点的领导者角色让给另一个节点，并将自己设置为候选人状态。
     *
     * @param newMaster 新的领导者节点。
     */
    private void abdicateTo(DiscoveryNode newMaster) {
        // 断言当前线程持有互斥锁。
        assert Thread.holdsLock(mutex);
        // 断言当前节点应该是领导者模式。
        assert mode == Mode.LEADER : "expected to be leader on abdication but was " + mode;
        // 断言新主节点应该是具有成为主节点资格的节点。
        assert newMaster.isMasterNode() : "should only abdicate to master-eligible node but was " + newMaster;

        // 创建一个新的开始加入请求，任期为当前任期和观察到的最大任期中的较大者加1。
        final StartJoinRequest startJoinRequest = new StartJoinRequest(newMaster, Math.max(getCurrentTerm(), maxTermSeen) + 1);

        // 记录退位给新主节点的日志。
        logger.info("abdicating to {} with term {}", newMaster, startJoinRequest.getTerm());

        // 遍历所有已知的主节点。
        getLastAcceptedState().nodes().mastersFirstStream().forEach(node -> {
            // 如果节点不是Zen1节点，则向该节点发送开始加入请求。
            if (isZen1Node(node) == false) {
                joinHelper.sendStartJoinRequest(startJoinRequest, node);
            }
        });

        // 处理本地节点上的开始加入消息将被分派到通用线程池。
        // 断言发送退位消息后当前节点仍然应该是领导者模式。
        assert mode == Mode.LEADER : "should still be leader after sending abdication messages " + mode;

        // 显式地将节点状态更改为候选人，以便下一个集群状态更新任务触发onNoLongerMaster事件。
        becomeCandidate("after abdicating to " + newMaster);
    }

    /**
     * 检查选举法定人数是否包含本地节点。
     *
     * @param lastAcceptedState 最后被接受的集群状态。
     * @return 如果法定人数包含本地节点，则返回true，否则返回false。
     */
    private static boolean electionQuorumContainsLocalNode(ClusterState lastAcceptedState) {
        // 获取最后被接受的集群状态中的本地节点。
        final DiscoveryNode localNode = lastAcceptedState.nodes().getLocalNode();
        // 断言本地节点不为null。
        assert localNode != null;
        // 调用另一个静态方法来检查选举法定人数是否包含指定的节点。
        return electionQuorumContains(lastAcceptedState, localNode);
    }

    /**
     * 检查选举法定人数是否包含指定的节点。
     *
     * @param lastAcceptedState 最后被接受的集群状态。
     * @param node 要检查的节点。
     * @return 如果法定人数包含指定的节点，则返回true，否则返回false。
     */
    private static boolean electionQuorumContains(ClusterState lastAcceptedState, DiscoveryNode node) {
        // 获取节点的ID。
        final String nodeId = node.getId();
        // 检查节点ID是否存在于最后提交的配置或最后接受的配置的节点ID列表中。
        return lastAcceptedState.getLastCommittedConfiguration().getNodeIds().contains(nodeId)
            || lastAcceptedState.getLastAcceptedConfiguration().getNodeIds().contains(nodeId);
    }

    /**
     * 确保当前任期至少达到目标任期，如果不是，则加入领导者。
     *
     * @param sourceNode 触发任期更新的源节点。
     * @param targetTerm 目标任期。
     * @return 如果任期更新导致加入领导者，则返回包含Join对象的Optional；如果当前任期已满足或更高，则返回空的Optional。
     */
    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        // 如果当前任期小于目标任期，则创建一个新的StartJoinRequest并加入领导者。
        if (getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        // 如果当前任期已经满足或高于目标任期，则返回空的Optional。
        return Optional.empty();
    }

    /**
     * 加入指定任期中的领导者。
     * 此方法处理加入请求，并更新节点状态以反映新的任期和加入信息。
     *
     * @param startJoinRequest 包含领导者节点和目标任期的开始加入请求。
     * @return 返回一个Join对象，包含加入操作的结果。
     */
    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 记录日志，显示正在加入的领导者节点和任期。
            logger.debug("joinLeaderInTerm: for [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());

            // 处理开始加入请求，并获取Join对象。
            final Join join = coordinationState.get().handleStartJoin(startJoinRequest);

            // 更新lastJoin为当前的Join对象。
            lastJoin = Optional.of(join);

            // 将peerFinder中的当前任期设置为最新的任期。
            peerFinder.setCurrentTerm(getCurrentTerm());

            // 检查当前模式是否不是候选人模式，如果不是，则变为候选人模式。
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("joinLeaderInTerm"); // 这会更新followersChecker和preVoteCollector
            } else {
                // 如果已经是候选人模式，则更新快速响应状态和预投票收集器。
                followersChecker.updateFastResponseState(getCurrentTerm(), mode);
                preVoteCollector.update(getPreVoteResponse(), null);
            }

            // 返回Join对象，包含加入领导者的结果。
            return join;
        }
    }


    /**
     * 处理来自其他节点的加入请求。
     *
     * @param joinRequest 加入请求，包含请求节点和相关信息。
     * @param joinCallback 回调接口，用于在处理完成后发送响应或失败通知。
     */
    private void handleJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        // 断言当前线程不持有互斥锁。
        assert Thread.holdsLock(mutex) == false;
        // 断言本地节点具有成为主节点的资格。
        assert getLocalNode().isMasterNode() : getLocalNode() + " received a join but is not master-eligible";

        // 记录日志，显示当前模式和正在处理的加入请求。
        logger.trace("handleJoinRequest: as {}, handling {}", mode, joinRequest);

        // 如果是单节点发现模式并且请求节点不是本地节点，则调用回调的onFailure方法。
        if (singleNodeDiscovery && joinRequest.getSourceNode().equals(getLocalNode()) == false) {
            joinCallback.onFailure(new IllegalStateException("cannot join node with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() +
                "] set to [" + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE  + "] discovery"));
            return;
        }

        // 连接到请求节点。
        transportService.connectToNode(joinRequest.getSourceNode());

        // 获取当前集群状态，用于加入验证。
        final ClusterState stateForJoinValidation = getStateForMasterService();

        // 如果本地节点已被选举为主节点，则进行一系列验证。
        if (stateForJoinValidation.nodes().isLocalNodeElectedMaster()) {
            // 遍历onJoinValidators并接受验证。
            onJoinValidators.forEach(a -> a.accept(joinRequest.getSourceNode(), stateForJoinValidation));
            // 检查集群状态是否有全局未恢复的块。
            if (stateForJoinValidation.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                // 确保请求节点的版本至少与集群中最小节点版本一致。
                // 这在多个地方执行，这里主要是为了尽快失败。
                JoinTaskExecutor.ensureMajorVersionBarrier(joinRequest.getSourceNode().getVersion(),
                    stateForJoinValidation.getNodes().getMinNodeVersion());
            }
            // 发送验证加入请求。
            sendValidateJoinRequest(stateForJoinValidation, joinRequest, joinCallback);

        } else {
            // 如果本地节点未被选举为主节点，则处理加入请求。
            processJoinRequest(joinRequest, joinCallback);
        }
    }

    // package private for tests
    /**
     * 发送验证加入请求。
     * 此方法在接收到一个节点的加入请求时调用，用于验证该节点是否可以加入当前集群。
     *
     * @param stateForJoinValidation 用于验证的集群状态。
     * @param joinRequest 加入请求，包含请求节点和相关信息。
     * @param joinCallback 回调接口，用于在处理完成后发送响应或失败通知。
     */
    void sendValidateJoinRequest(ClusterState stateForJoinValidation, JoinRequest joinRequest,
                                 JoinHelper.JoinCallback joinCallback) {
        // 使用joinHelper发送验证请求到请求加入的节点。
        // 这个操作会异步执行，结果通过ActionListener<Empty>回调处理。
        joinHelper.sendValidateJoinRequest(joinRequest.getSourceNode(), stateForJoinValidation, new ActionListener<Empty>() {
            // 当验证请求成功响应时调用此方法。
            @Override
            public void onResponse(Empty empty) {
                try {
                    // 如果验证成功，进一步处理加入请求。
                    processJoinRequest(joinRequest, joinCallback);
                } catch (Exception e) {
                    // 如果处理加入请求时出现异常，通过回调通知失败。
                    joinCallback.onFailure(e);
                }
            }

            // 如果验证请求失败或超时，调用此方法。
            @Override
            public void onFailure(Exception e) {
                // 记录警告日志，显示验证失败的节点和异常信息。
                logger.warn(() -> new ParameterizedMessage("failed to validate incoming join request from node [{}]",
                    joinRequest.getSourceNode()), e);

                // 通过回调通知验证请求失败，并提供异常信息。
                joinCallback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
            }
        });
    }

    /**
     * 处理加入请求。
     * 当一个节点请求加入集群时，此方法会被调用以处理该请求。
     *
     * @param joinRequest 加入请求，包含请求节点和相关信息。
     * @param joinCallback 回调接口，用于在处理完成后发送响应或失败通知。
     */
    private void processJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        // 获取加入请求中可能包含的可选Join对象。
        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();

        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 获取当前协调状态。
            final CoordinationState coordState = coordinationState.get();

            // 记录之前是否赢得了选举。
            final boolean prevElectionWon = coordState.electionWon();

            // 如果存在Join对象，则处理它。
            optionalJoin.ifPresent(this::handleJoin);

            // 处理加入请求累积器的逻辑。
            joinAccumulator.handleJoinRequest(joinRequest.getSourceNode(), joinCallback);

            // 如果之前没有赢得选举，但现在赢得了，则变为领导者。
            if (prevElectionWon == false && coordState.electionWon()) {
                becomeLeader("handleJoinRequest");
            }
        }
    }

    /**
     * 将当前节点状态设置为候选人。
     *
     * @param method 触发成为候选人状态的方法名称，用于日志记录。
     */
    void becomeCandidate(String method) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

        // 记录日志，显示当前方法、任期、之前和最后已知的领导者模式。
        logger.debug("{}: coordinator becoming CANDIDATE in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

        // 检查当前模式是否不是候选人模式，如果不是，则执行状态转换逻辑。
        if (mode != Mode.CANDIDATE) {
            // 记录之前的模式。
            final Mode prevMode = mode;
            // 设置当前模式为候选人。
            mode = Mode.CANDIDATE;

            // 取消任何活跃的发布操作。
            cancelActivePublication("become candidate: " + method);

            // 关闭现有的加入累加器并创建一个新的候选人加入累加器。
            joinAccumulator.close(mode);
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            // 激活peerFinder，使用最后接受的集群状态中的节点列表。
            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());

            // 启动集群形成失败帮助器。
            clusterFormationFailureHelper.start();

            // 如果当前任期是向后兼容的任期，则激活发现服务升级。
            if (getCurrentTerm() == ZEN1_BWC_TERM) {
                discoveryUpgradeService.activate(lastKnownLeader, coordinationState.get().getLastAcceptedState());
            }

            // 更新leaderChecker为没有当前节点，并将领导者设置为null。
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
            leaderChecker.updateLeader(null);

            // 清除followersChecker中的当前节点并更新快速响应状态。
            followersChecker.clearCurrentNodes();
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);

            // 清除lagDetector跟踪的节点。
            lagDetector.clearTrackedNodes();

            // 如果之前是领导者模式，则清理主服务。
            if (prevMode == Mode.LEADER) {
                cleanMasterService();
            }

            // 如果应用者状态有主节点ID，则更新应用者状态为没有主节点，并应用新的集群状态。
            if (applierState.nodes().getMasterNodeId() != null) {
                applierState = clusterStateWithNoMasterBlock(applierState);
                clusterApplier.onNewClusterState("becoming candidate: " + method, () -> applierState, (source, e) -> {
                });
            }
        }

        // 更新预投票收集器。
        preVoteCollector.update(getPreVoteResponse(), null);
    }

    /**
     * 将当前节点状态设置为领导者。
     *
     * @param method 触发成为领导者状态的方法名称，用于日志记录。
     */
    void becomeLeader(String method) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        // 断言当前模式应为候选人模式。
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        // 断言本地节点应具有成为主节点的资格。
        assert getLocalNode().isMasterNode() : getLocalNode() + " became a leader but is not master-eligible";

        // 记录日志，显示当前方法、任期、之前和最后已知的领导者模式。
        logger.debug("{}: coordinator becoming LEADER in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

        // 设置当前模式为领导者。
        mode = Mode.LEADER;

        // 关闭现有的加入累加器并创建一个新的领导者加入累加器。
        joinAccumulator.close(mode);
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        // 更新最后已知的领导者为本地节点。
        lastKnownLeader = Optional.of(getLocalNode());

        // 停用peerFinder，因为当前节点已成为领导者。
        peerFinder.deactivate(getLocalNode());

        // 停用发现服务升级。
        discoveryUpgradeService.deactivate();

        // 停止集群形成失败帮助器。
        clusterFormationFailureHelper.stop();

        // 关闭预投票和选举调度器。
        closePrevotingAndElectionScheduler();

        // 更新预投票收集器，将当前节点设置为领导者。
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());

        // 断言当前没有其他领导者。
        assert leaderChecker.leader() == null : leaderChecker.leader();

        // 更新追随者检查器的快速响应状态。
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
    }

    /**
     * 将当前节点状态设置为跟随者。
     *
     * @param method 触发成为跟随者状态的方法名称，用于日志记录。
     * @param leaderNode 领导者节点。
     */
    void becomeFollower(String method, DiscoveryNode leaderNode) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        // 断言领导者节点具有成为主节点的资格。
        assert leaderNode.isMasterNode() : leaderNode + " became a leader but is not master-eligible";
        // 断言当前模式不是领导者模式，因为跟随者状态转换应该发生在候选人模式之后。
        assert mode != Mode.LEADER : "do not switch to follower from leader (should be candidate first)";

        // 如果当前已经是跟随者模式，并且领导者节点没有变化，则记录跟踪日志。
        if (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) {
            logger.trace("{}: coordinator remaining FOLLOWER of [{}] in term {}",
                method, leaderNode, getCurrentTerm());
        } else {
            // 如果模式或领导者节点发生变化，记录调试日志。
            logger.debug("{}: coordinator becoming FOLLOWER of [{}] in term {} (was {}, lastKnownLeader was [{}])",
                method, leaderNode, getCurrentTerm(), mode, lastKnownLeader);
        }

        // 检查是否需要重启领导者检查器。
        final boolean restartLeaderChecker = (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) == false;

        // 如果当前不是跟随者模式，执行跟随者模式的初始化操作。
        if (mode != Mode.FOLLOWER) {
            mode = Mode.FOLLOWER; // 设置模式为跟随者。
            joinAccumulator.close(mode); // 关闭当前加入累加器。
            joinAccumulator = new JoinHelper.FollowerJoinAccumulator(); // 创建跟随者加入累加器。
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES); // 重置领导者检查器的当前节点列表。
        }

        // 更新最后已知的领导者为传入的领导者节点。
        lastKnownLeader = Optional.of(leaderNode);
        // 停用针对指定领导者节点的peerFinder。
        peerFinder.deactivate(leaderNode);
        // 停用发现服务升级。
        discoveryUpgradeService.deactivate();
        // 停止集群形成失败帮助器。
        clusterFormationFailureHelper.stop();
        // 关闭预投票和选举调度器。
        closePrevotingAndElectionScheduler();
        // 取消任何活跃的发布操作。
        cancelActivePublication("become follower: " + method);
        // 更新预投票收集器，将当前节点设置为跟随者。
        preVoteCollector.update(getPreVoteResponse(), leaderNode);

        // 如果需要重启领导者检查器，更新领导者检查器的领导者节点。
        if (restartLeaderChecker) {
            leaderChecker.updateLeader(leaderNode);
        }

        // 清除追随者检查器的当前节点列表并更新快速响应状态。
        followersChecker.clearCurrentNodes();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
        // 清除lagDetector跟踪的节点。
        lagDetector.clearTrackedNodes();
    }

    /**
     * 清理主服务。
     * 当前节点不再担任主节点时，调用此方法来执行清理操作。
     */
    private void cleanMasterService() {
        // 使用masterService提交一个状态更新任务，用于在不再担任主节点后进行清理。
        masterService.submitStateUpdateTask("clean-up after stepping down as master", new LocalClusterUpdateTask() {
            @Override
            public void onFailure(String source, Exception e) {
                // 如果清理任务失败，记录一条跟踪日志，但忽略异常。
                // 这里选择忽略可能是由于清理操作不是关键性任务，或者异常处理已经在其他地方完成。
                logger.trace("failed to clean-up after stepping down as master", e);
            }

            @Override
            public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                // 检查当前状态中本地节点是否不再是选举出的主节点。
                if (currentState.nodes().isLocalNodeElectedMaster() == false) {
                    // 如果本地节点不是主节点，调用allocationService来清理缓存。
                    allocationService.cleanCaches();
                }
                // 返回未改变的状态，表示执行任务后集群状态没有变化。
                return unchanged();
            }
        });
    }

    /**
     * 获取预投票响应。
     * 创建并返回一个新的PreVoteResponse对象，包含当前任期、最后接受的任期和版本信息。
     *
     * @return 预投票响应对象。
     */
    private PreVoteResponse getPreVoteResponse() {
        // 创建并返回一个新的PreVoteResponse对象，包含当前任期、最后接受的任期和版本信息。
        return new PreVoteResponse(
            getCurrentTerm(), // 当前任期
            coordinationState.get().getLastAcceptedTerm(), // 最后接受的任期
            coordinationState.get().getLastAcceptedState().getVersionOrMetaDataVersion() // 版本或元数据版本
        );
    }

    /**
     * 获取当前任期。
     * 需要同步互斥锁来确保线程安全。
     *
     * @return 当前任期。
     */
    long getCurrentTerm() {
        synchronized (mutex) {
            return coordinationState.get().getCurrentTerm();
        }
    }

    /**
     * 获取当前模式。
     * 需要同步互斥锁来确保线程安全。
     *
     * @return 当前模式。
     */
    Mode getMode() {
        synchronized (mutex) {
            return mode;
        }
    }

    /**
     * 获取本地节点信息。
     * 此方法不要求同步，因为假设transportService的本地节点获取是线程安全的。
     *
     * @return 本地节点信息。
     */
    DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
    }

    /**
     * 检查是否有发布操作正在进行。
     * 需要同步互斥锁来确保线程安全。
     *
     * @return 如果有发布操作正在进行返回true，否则返回false。
     */
    boolean publicationInProgress() {
        synchronized (mutex) {
            return currentPublication.isPresent();
        }
    }
    /**
     * 执行启动操作。
     * 此方法在组件启动时被调用，用于设置初始状态和启动相关服务。
     */
    @Override
    protected void doStart() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 获取持久化的协调状态。
            CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
            // 使用持久化状态和本地节点信息初始化协调状态。
            coordinationState.set(new CoordinationState(settings, getLocalNode(), persistedState));
            // 设置peerFinder的当前任期。
            peerFinder.setCurrentTerm(getCurrentTerm());
            // 启动配置的主机解析器。
            configuredHostsResolver.start();

            // 获取最后接受的集群状态。
            final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
            // 如果集群UUID已提交，记录集群UUID。
            if (lastAcceptedState.metaData().clusterUUIDCommitted()) {
                logger.info("cluster UUID [{}]", lastAcceptedState.metaData().clusterUUID());
            }

            // 获取最后提交的投票配置。
            final VotingConfiguration votingConfiguration = lastAcceptedState.getLastCommittedConfiguration();

            // 如果是单节点发现模式，并且投票配置不为空且本地节点不在法定人数中，则抛出异常。
            if (singleNodeDiscovery &&
                votingConfiguration.isEmpty() == false &&
                votingConfiguration.hasQuorum(Collections.singleton(getLocalNode().getId())) == false) {
                throw new IllegalStateException("cannot start with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() + "] set to [" +
                    DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE + "] when local node " + getLocalNode() +
                    " does not have quorum in voting configuration " + votingConfiguration);
            }

            // 构建初始集群状态。
            ClusterState initialState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK) // 添加未恢复状态的全局块。
                    .addGlobalBlock(noMasterBlockService.getNoMasterBlock())) // 添加无主节点的全局块。
                .nodes(DiscoveryNodes.builder().add(getLocalNode()).localNodeId(getLocalNode().getId())) // 添加本地节点。
                .build();

            // 设置应用者状态为初始集群状态。
            applierState = initialState;
            // 设置集群应用器的初始状态。
            clusterApplier.setInitialState(initialState);
        }
    }

    /**
     * 获取发现服务的统计信息。
     *
     * @return 返回包含待处理集群状态统计信息和发布处理程序统计信息的DiscoveryStats对象。
     */
    @Override
    public DiscoveryStats stats() {
        // 创建并返回一个新的DiscoveryStats对象，包含待处理集群状态的统计信息和发布处理程序的统计信息。
        return new DiscoveryStats(
            new PendingClusterStateStats(0, 0, 0), // 待处理集群状态统计信息，这里传入0表示没有待处理状态。
            publicationHandler.stats() // 发布处理程序的统计信息。
        );
    }

    /**
     * 开始初始加入流程。
     * 此方法被调用以启动节点加入集群的初始流程。
     */
    @Override
    public void startInitialJoin() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 将当前节点设置为候选人模式，准备开始加入流程。
            becomeCandidate("startInitialJoin");
        }
        // 调度未配置的引导服务，以便开始集群形成过程。
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
    }

    /**
     * 执行停止操作。
     * 此方法在组件停止时被调用，用于执行必要的清理操作。
     */
    @Override
    protected void doStop() {
        // 停止配置的主机解析器。
        configuredHostsResolver.stop();
    }

    /**
     * 执行关闭操作。
     * 此方法在组件关闭时被调用，用于释放资源。
     */
    @Override
    protected void doClose() {
        // 目前没有实现关闭逻辑，可能在子类中实现或留空。
    }

    /**
     * 检查并确保协调服务的状态满足不变性条件。
     */
    public void invariant() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 获取peerFinder中的领导者节点。
            final Optional<DiscoveryNode> peerFinderLeader = peerFinder.getLeader();

            // 检查当前任期是否与peerFinder中的任期一致。
            assert peerFinder.getCurrentTerm() == getCurrentTerm();

            // 检查followersChecker中的快速响应状态的任期和模式是否与当前任期和模式一致。
            assert followersChecker.getFastResponseState().term == getCurrentTerm() : followersChecker.getFastResponseState();
            assert followersChecker.getFastResponseState().mode == getMode() : followersChecker.getFastResponseState();

            // 检查是否有主节点，以及集群UUID是否已提交。
            assert (applierState.nodes().getMasterNodeId() == null) == applierState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID);
            assert applierState.nodes().getMasterNodeId() == null || applierState.metaData().clusterUUIDCommitted();

            // 检查预投票收集器中的预投票响应是否与当前节点的预投票响应一致。
            assert preVoteCollector.getPreVoteResponse().equals(getPreVoteResponse())
                : preVoteCollector + " vs " + getPreVoteResponse();

            // 检查本地节点是否没有被lagDetector跟踪。
            assert lagDetector.getTrackedNodes().contains(getLocalNode()) == false : lagDetector.getTrackedNodes();

            // 检查followersChecker中的已知跟随者是否与lagDetector跟踪的节点一致。
            assert followersChecker.getKnownFollowers().equals(lagDetector.getTrackedNodes())
                : followersChecker.getKnownFollowers() + " vs " + lagDetector.getTrackedNodes();

            // 根据不同的模式（领导者、跟随者、候选人），执行更具体的不变性检查。
            if (mode == Mode.LEADER) {
                final boolean becomingMaster = getStateForMasterService().term() != getCurrentTerm();

                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
                assert joinAccumulator instanceof JoinHelper.LeaderJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert becomingMaster || getStateForMasterService().nodes().getMasterNodeId() != null : getStateForMasterService();
                assert leaderChecker.leader() == null : leaderChecker.leader();
                assert getLocalNode().equals(applierState.nodes().getMasterNode()) ||
                    (applierState.nodes().getMasterNodeId() == null && applierState.term() < getCurrentTerm());
                assert preVoteCollector.getLeader() == getLocalNode() : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning() == false;

                final boolean activePublication = currentPublication.map(CoordinatorPublication::isActiveForCurrentLeader).orElse(false);
                if (becomingMaster && activePublication == false) {
                    // cluster state update task to become master is submitted to MasterService, but publication has not started yet
                    assert followersChecker.getKnownFollowers().isEmpty() : followersChecker.getKnownFollowers();
                } else {
                    final ClusterState lastPublishedState;
                    if (activePublication) {
                        // active publication in progress: followersChecker is up-to-date with nodes that we're actively publishing to
                        lastPublishedState = currentPublication.get().publishedState();
                    } else {
                        // no active publication: followersChecker is up-to-date with the nodes of the latest publication
                        lastPublishedState = coordinationState.get().getLastAcceptedState();
                    }
                    final Set<DiscoveryNode> lastPublishedNodes = new HashSet<>();
                    lastPublishedState.nodes().forEach(lastPublishedNodes::add);
                    assert lastPublishedNodes.remove(getLocalNode()); // followersChecker excludes local node
                    assert lastPublishedNodes.equals(followersChecker.getKnownFollowers()) :
                        lastPublishedNodes + " != " + followersChecker.getKnownFollowers();
                }

                assert becomingMaster || activePublication ||
                    coordinationState.get().getLastAcceptedConfiguration().equals(coordinationState.get().getLastCommittedConfiguration())
                    : coordinationState.get().getLastAcceptedConfiguration() + " != "
                    + coordinationState.get().getLastCommittedConfiguration();
            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
                assert joinAccumulator instanceof JoinHelper.FollowerJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert lastKnownLeader.equals(Optional.of(leaderChecker.leader()));
                assert followersChecker.getKnownFollowers().isEmpty();
                assert lastKnownLeader.get().equals(applierState.nodes().getMasterNode()) ||
                    (applierState.nodes().getMasterNodeId() == null &&
                        (applierState.term() < getCurrentTerm() || applierState.version() < getLastAcceptedState().version()));
                assert currentPublication.map(Publication::isCommitted).orElse(true);
                assert preVoteCollector.getLeader().equals(lastKnownLeader.get()) : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning() == false;
            } else {
                assert mode == Mode.CANDIDATE;
                assert joinAccumulator instanceof JoinHelper.CandidateJoinAccumulator;
                assert peerFinderLeader.isPresent() == false : peerFinderLeader;
                assert prevotingRound == null || electionScheduler != null;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert leaderChecker.leader() == null : leaderChecker.leader();
                assert followersChecker.getKnownFollowers().isEmpty();
                assert applierState.nodes().getMasterNodeId() == null;
                assert currentPublication.map(Publication::isCommitted).orElse(true);
                assert preVoteCollector.getLeader() == null : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning();
            }
        }
    }

    /**
     * 检查是否已设置初始配置。
     * 在分布式系统中，此方法用于确定是否已有节点加入并形成了初始的集群配置。
     *
     * @return 如果已设置初始配置，则返回true；否则返回false。
     */
    public boolean isInitialConfigurationSet() {
        // 获取用于主服务的集群状态。
        ClusterState stateForMasterService = getStateForMasterService();
        // 检查最后接受的配置是否为空。
        // 如果不为空，表示已有节点加入，形成了集群的初始配置。
        return stateForMasterService.getLastAcceptedConfiguration().isEmpty() == false;
    }

    /**
     * 设置初始配置为给定的投票配置。
     * 此方法可以安全地多次调用，只要每次调用时传入的参数相同。
     *
     * @param votingConfiguration 形成初始配置的节点集合。
     * @return 如果此调用成功设置了初始配置，则返回true；如果返回false，则表示集群已经被引导启动。
     */
    public boolean setInitialConfiguration(final VotingConfiguration votingConfiguration) {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 获取用于主服务的当前集群状态。
            final ClusterState currentState = getStateForMasterService();

            // 如果初始配置已经设置，则记录调试日志并忽略此次设置。
            if (isInitialConfigurationSet()) {
                logger.debug("initial configuration already set, ignoring {}", votingConfiguration);
                return false;
            }

            // 如果本地节点不是主节点，则记录调试日志并抛出异常。
            if (getLocalNode().isMasterNode() == false) {
                logger.debug("skip setting initial configuration as local node is not a master-eligible node");
                throw new CoordinationStateRejectedException(
                    "this node is not master-eligible, but cluster bootstrapping can only happen on a master-eligible node");
            }

            // 如果本地节点不在初始配置中，则记录调试日志并抛出异常。
            if (votingConfiguration.getNodeIds().contains(getLocalNode().getId()) == false) {
                logger.debug("skip setting initial configuration as local node is not part of initial configuration");
                throw new CoordinationStateRejectedException("local node is not part of initial configuration");
            }

            // 获取已知节点列表，包括本地节点和已发现的对等节点。
            final List<DiscoveryNode> knownNodes = new ArrayList<>();
            knownNodes.add(getLocalNode());
            peerFinder.getFoundPeers().forEach(knownNodes::add);

            // 如果已知节点中不足以形成初始配置的法定人数，则记录调试日志并抛出异常。
            if (votingConfiguration.hasQuorum(knownNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList())) == false) {
                logger.debug("skip setting initial configuration as not enough nodes discovered to form a quorum in the " +
                    "initial configuration [knownNodes={}, {}]", knownNodes, votingConfiguration);
                throw new CoordinationStateRejectedException("not enough nodes discovered to form a quorum in the initial configuration " +
                    "[knownNodes=" + knownNodes + ", " + votingConfiguration + "]");
            }

            // 记录信息日志，显示正在设置的初始配置。
            logger.info("setting initial configuration to {}", votingConfiguration);
            // 构建新的协调元数据，包含初始配置。
            final CoordinationMetaData coordinationMetaData = CoordinationMetaData.builder(currentState.coordinationMetaData())
                .lastAcceptedConfiguration(votingConfiguration)
                .lastCommittedConfiguration(votingConfiguration)
                .build();

            // 构建新的元数据，包含协调元数据。
            MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
            // 自动生成元数据的集群UUID（如果需要）。
            metaDataBuilder.generateClusterUuidIfNeeded(); // TODO: 在引导工具中生成UUID？
            metaDataBuilder.coordinationMetaData(coordinationMetaData);

            // 设置协调状态的初始状态，包含新的元数据。
            coordinationState.get().setInitialState(ClusterState.builder(currentState).metaData(metaDataBuilder).build());
            // 断言本地节点在选举法定人数中。
            assert electionQuorumContainsLocalNode(getLastAcceptedState()) :
                "initial state does not have local node in its election quorum: " + getLastAcceptedState().coordinationMetaData();
            // 更新预投票收集器，以反映最后接受状态的版本变化。
            preVoteCollector.update(getPreVoteResponse(), null);
            // 启动选举调度器。
            startElectionScheduler();
            // 返回true，表示成功设置了初始配置。
            return true;
        }
    }
    /**
     * 改进集群配置。
     * 此方法尝试基于当前活跃节点和一些排除条件来更新集群的投票配置。
     *
     * @param clusterState 当前集群状态。
     * @return 改进后的集群状态，如果配置没有变化则返回原状态。
     */
    ClusterState improveConfiguration(ClusterState clusterState) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

        // 从当前集群状态中筛选出活跃节点，排除那些没有来自当前节点的加入投票的节点和Zen1节点。
        final Set<DiscoveryNode> liveNodes = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(this::hasJoinVoteFrom) // 筛选出有当前节点投票的节点。
            .filter(discoveryNode -> isZen1Node(discoveryNode) == false) // 排除Zen1节点。
            .collect(Collectors.toSet());

        // 使用reconfigurator来重新配置投票配置，考虑排除的节点和当前节点。
        final VotingConfiguration newConfig = reconfigurator.reconfigure(
            liveNodes, // 当前活跃节点。
            clusterState.getVotingConfigExclusions().stream().map(VotingConfigExclusion::getNodeId).collect(Collectors.toSet()), // 排除的节点ID集合。
            getLocalNode(), // 当前节点。
            clusterState.getLastAcceptedConfiguration() // 当前集群的最后接受的配置。
        );

        // 如果新配置与当前配置不同，则断言新的配置有足够的法定人数支持。
        if (newConfig.equals(clusterState.getLastAcceptedConfiguration()) == false) {
            assert coordinationState.get().joinVotesHaveQuorumFor(newConfig);

            // 构建并返回改进后的集群状态，包含新的协调元数据。
            return ClusterState.builder(clusterState)
                .metaData(MetaData.builder(clusterState.metaData())
                    .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData())
                        .lastAcceptedConfiguration(newConfig) // 设置新的最后接受的配置。
                        .build()))
                .build();
        }

        // 如果新配置与当前配置相同，返回原始集群状态。
        return clusterState;
    }

    /**
     * 表示是否已经安排了重配置任务的原子布尔变量。
     * 使用AtomicBoolean是为了在多线程环境中安全地检查和设置状态，而不需要额外的同步。
     */
    private AtomicBoolean reconfigurationTaskScheduled = new AtomicBoolean();

    private void scheduleReconfigurationIfNeeded() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.LEADER : mode;
        assert currentPublication.isPresent() == false : "Expected no publication in progress";

        final ClusterState state = getLastAcceptedState();
        if (improveConfiguration(state) != state && reconfigurationTaskScheduled.compareAndSet(false, true)) {
            logger.trace("scheduling reconfiguration");
            masterService.submitStateUpdateTask("reconfigure", new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    reconfigurationTaskScheduled.set(false);
                    synchronized (mutex) {
                        return improveConfiguration(currentState);
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    reconfigurationTaskScheduled.set(false);
                    logger.debug("reconfiguration failed", e);
                }
            });
        }
    }

// 仅供测试使用
    /**
     * 检查是否从指定节点收到了加入投票。
     *
     * @param node 要检查的节点。
     * @return 如果从该节点收到了加入投票，则返回true。
     */
    boolean hasJoinVoteFrom(DiscoveryNode node) {
        return coordinationState.get().containsJoinVoteFor(node);
    }

    /**
     * 处理节点加入请求。
     * 此方法在接收到节点的加入请求时被调用。
     *
     * @param join 加入请求。
     */
    private void handleJoin(Join join) {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 确保当前任期至少与加入请求中的任期一致，如果有更新，则处理加入请求。
            ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(this::handleJoin);

            // 如果我们已经赢得了选举，则实际的加入请求对选举结果没有影响，因此可以忽略任何异常。
            if (coordinationState.get().electionWon()) {
                // 处理加入请求，忽略可能的异常。
                final boolean isNewJoin = handleJoinIgnoringExceptions(join);

                // 如果我们已经完全成为主节点，并且没有发布操作正在进行，则根据需要安排重配置任务。
                final boolean establishedAsMaster = mode == Mode.LEADER && getLastAcceptedState().term() == getCurrentTerm();
                if (isNewJoin && establishedAsMaster && publicationInProgress() == false) {
                    scheduleReconfigurationIfNeeded();
                }
            } else {
                // 处理加入请求，这可能会失败并抛出异常。
                coordinationState.get().handleJoin(join);
            }
        }
    }

    /**
     * 处理节点加入请求，忽略可能的异常。
     *
     * @param join 加入请求。
     * @return 如果加入请求来自新节点，并且已成功添加，则返回true。
     */
    private boolean handleJoinIgnoringExceptions(Join join) {
        try {
            // 处理加入请求。
            return coordinationState.get().handleJoin(join);
        } catch (CoordinationStateRejectedException e) {
            // 如果处理加入请求失败，记录调试日志并忽略。
            logger.debug("failed to add {} - ignoring", join, e);
            return false;
        }
    }

    /**
     * 获取最后被接受的集群状态。
     * 此方法返回协调状态中最后被接受的集群状态。
     *
     * @return 最后被接受的集群状态。
     */
    public ClusterState getLastAcceptedState() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            return coordinationState.get().getLastAcceptedState();
        }
    }

    /**
     * 获取应用者状态。
     * 此方法返回当前应用者状态，可能为null。
     *
     * @return 当前应用者状态，或者null。
     */
    @Nullable
    public ClusterState getApplierState() {
        return applierState;
    }

    /**
     * 获取已发现的节点列表。
     * 此方法返回包含本地节点和通过peerFinder发现的对等节点的列表。
     *
     * @return 已发现的节点列表。
     */
    private List<DiscoveryNode> getDiscoveredNodes() {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        // 添加本地节点。
        nodes.add(getLocalNode());
        // 遍历并添加通过peerFinder发现的对等节点。
        peerFinder.getFoundPeers().forEach(nodes::add);
        return nodes;
    }

    /**
     * 获取用于主服务的集群状态。
     * 此方法返回协调状态中最后被接受的集群状态，作为主服务计算下一个集群状态更新的基础。
     *
     * @return 用于主服务的集群状态。
     */
    ClusterState getStateForMasterService() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            final ClusterState clusterState = coordinationState.get().getLastAcceptedState();
            // 如果当前模式不是领导者，或者集群状态的任期与当前任期不一致，
            // 则返回带有无主节点块的集群状态。
            if (mode != Mode.LEADER || clusterState.term() != getCurrentTerm()) {
                return clusterStateWithNoMasterBlock(clusterState);
            }
            // 如果是领导者并且任期匹配，则直接返回集群状态。
            return clusterState;
        }
    }

    /**
     * 创建一个没有主节点的集群状态。
     * 如果原始集群状态包含主节点ID，则此方法会移除主节点ID并添加一个没有主节点的全局块。
     *
     * @param clusterState 原始集群状态。
     * @return 包含没有主节点块的新集群状态，或者如果原始状态中没有主节点ID，则返回原始状态。
     */
    private ClusterState clusterStateWithNoMasterBlock(ClusterState clusterState) {
        // 检查原始集群状态是否包含主节点ID。
        if (clusterState.nodes().getMasterNodeId() != null) {
            // 如果存在主节点ID，则添加没有主节点的全局块。
            // 断言确保在添加之前没有NO_MASTER_BLOCK。
            assert clusterState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID) == false :
                "NO_MASTER_BLOCK should only be added by Coordinator";

            // 创建一个新的ClusterBlocks，包含原始blocks并添加NO_MASTER_BLOCK。
            final ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks())
                .addGlobalBlock(noMasterBlockService.getNoMasterBlock()).build();

            // 创建一个新的DiscoveryNodes，没有设置主节点ID。
            final DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();

            // 构建并返回一个新的ClusterState，包含更新后的blocks和nodes。
            return ClusterState.builder(clusterState).blocks(clusterBlocks).nodes(discoveryNodes).build();
        } else {
            // 如果原始集群状态中没有主节点ID，直接返回原始状态。
            return clusterState;
        }
    }
    /**
     * 发布集群状态变更。
     * 当集群状态发生变化时，此方法会被调用以在集群中发布这些变更。
     *
     * @param clusterChangedEvent 集群状态变更事件，包含新的状态和变更源。
     * @param publishListener 发布操作的监听器，用于接收发布操作的结果。
     * @param ackListener 确认监听器，用于接收来自其他节点的确认信息。
     */
    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        try {
            // 进入同步块以确保线程安全。
            synchronized (mutex) {
                // 如果当前节点不是领导者，或者当前任期与集群状态的任期不匹配，则发布失败。
                if (mode != Mode.LEADER || getCurrentTerm() != clusterChangedEvent.state().term()) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed publication as node is no longer master for term {}",
                        clusterChangedEvent.source(), clusterChangedEvent.state().term()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("node is no longer master for term " +
                        clusterChangedEvent.state().term() + " while handling publication"));
                    return;
                }

                // 如果已经有发布操作正在进行，则发布失败。
                if (currentPublication.isPresent()) {
                    assert false : "[" + currentPublication.get() + "] in progress, cannot start new publication";
                    logger.warn(() -> new ParameterizedMessage("[{}] failed publication as already publication in progress",
                        clusterChangedEvent.source()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("publication " + currentPublication.get() +
                        " already in progress"));
                    return;
                }

                // 断言集群状态变更事件与之前的状态一致性。
                assert assertPreviousStateConsistency(clusterChangedEvent);

                final ClusterState clusterState = clusterChangedEvent.state();

                // 断言本地节点应该包含在要发布的集群状态中。
                assert getLocalNode().equals(clusterState.getNodes().get(getLocalNode().getId())) :
                    getLocalNode() + " should be in published " + clusterState;

                // 创建发布上下文。
                final PublicationTransportHandler.PublicationContext publicationContext =
                    publicationHandler.newPublicationContext(clusterChangedEvent);

                // 处理客户端的值，生成发布请求。
                final PublishRequest publishRequest = coordinationState.get().handleClientValue(clusterState);
                // 创建协调发布对象。
                final CoordinatorPublication publication = new CoordinatorPublication(publishRequest, publicationContext,
                    new ListenableFuture<>(), ackListener, publishListener);
                // 记录当前正在进行的发布操作。
                currentPublication = Optional.of(publication);

                // 获取要发布到的节点列表。
                final DiscoveryNodes publishNodes = publishRequest.getAcceptedState().nodes();
                // 设置领导者检查器和追随者检查器的当前节点列表。
                leaderChecker.setCurrentNodes(publishNodes);
                followersChecker.setCurrentNodes(publishNodes);
                // 设置延迟检测器跟踪的节点。
                lagDetector.setTrackedNodes(publishNodes);
                // 开始发布操作。
                publication.start(followersChecker.getFaultyNodes());
            }
        } catch (Exception e) {
            // 如果发布过程中发生异常，记录异常并通知发布失败。
            logger.debug(() -> new ParameterizedMessage("[{}] publishing failed", clusterChangedEvent.source()), e);
            publishListener.onFailure(new FailedToCommitClusterStateException("publishing failed", e));
        }
    }

    /**
     * 断言之前状态的一致性。
     * 由于ClusterState没有equals方法，因此通过序列化它到XContent并比较得到的Map来比较一致性。
     *
     * @param event 集群状态变更事件，包含之前的状态和新的状态。
     * @return 如果之前的状态与协调状态中的最后接受状态一致，则返回true。
     */
    private boolean assertPreviousStateConsistency(ClusterChangedEvent event) {
        // 使用assert断言之前的状态与协调状态中的最后接受状态一致，
        // 或者序列化之前的状态和最后接受状态（添加无主节点块）到JSON，并比较得到的Map。
        assert event.previousState() == coordinationState.get().getLastAcceptedState() ||
            XContentHelper.convertToMap(
                JsonXContent.jsonXContent, Strings.toString(event.previousState()), false
            ).equals(
                XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState())),
                    false
                )
            ) : "Expected previous state does not match last accepted state: " +
            Strings.toString(event.previousState()) + " vs " +
            Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState()));
        // 如果断言通过，则返回true。
        return true;
    }

    /**
     * 包装ActionListener以确保线程安全。
     * 使用互斥锁(mutex)来同步回调方法，保证在多线程环境中的同步访问。
     *
     * @param listener 要包装的ActionListener对象。
     * @param <T> 泛型类型，表示ActionListener处理的结果类型。
     * @return 返回一个新的ActionListener，其onResponse和onFailure方法在执行时持有互斥锁。
     */
    private <T> ActionListener<T> wrapWithMutex(ActionListener<T> listener) {
        // 创建一个新的ActionListener，包装传入的listener。
        return new ActionListener<T>() {
            /**
             * 当操作成功并返回响应时调用。
             * 在调用原始listener的onResponse之前，先进入同步块以确保线程安全。
             *
             * @param t 返回的结果。
             */
            @Override
            public void onResponse(T t) {
                synchronized (mutex) {
                    // 调用原始listener的onResponse方法。
                    listener.onResponse(t);
                }
            }

            /**
             * 当操作失败时调用。
             * 在调用原始listener的onFailure之前，先进入同步块以确保线程安全。
             *
             * @param e 异常对象，表示失败的原因。
             */
            @Override
            public void onFailure(Exception e) {
                synchronized (mutex) {
                    // 调用原始listener的onFailure方法。
                    listener.onFailure(e);
                }
            }
        };
    }

    /**
     * 取消当前激活的发布操作。
     * 如果存在当前激活的发布，将调用其取消方法，并提供一个取消的原因。
     *
     * @param reason 取消发布操作的描述性原因。
     */
    private void cancelActivePublication(String reason) {
        // 断言以确保当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        // 检查是否存在当前发布对象。
        if (currentPublication.isPresent()) {
            // 如果存在，调用其cancel方法，并传递取消的原因。
            currentPublication.get().cancel(reason);
        }
    }

    /**
     * 获取用于加入集群时验证的BiConsumer集合。
     * 这些验证器是用于在节点加入集群时对节点和集群状态进行验证的函数接口。
     *
     * @return 返回一个包含所有加入验证器的集合。
     */
    public Collection<BiConsumer<DiscoveryNode, ClusterState>> getOnJoinValidators() {
        // 返回onJoinValidators集合，它包含节点和集群状态的验证逻辑。
        return onJoinValidators;
    }

    /**
     * 定义节点在分布式系统中可能处于的模式。
     */
    public enum Mode {
        CANDIDATE, // 候选人模式，表示节点可以参与领导者选举。
        LEADER,   // 领导者模式，表示节点是集群的领导者。
        FOLLOWER   // 跟随者模式，表示节点遵循领导者的指令。
    }
    /**
     * 协调器使用的PeerFinder扩展类。
     * 此类负责发现网络中的对等节点，并根据当前的模式和状态处理发现的节点。
     */
    private class CoordinatorPeerFinder extends PeerFinder {

        /**
         * 构造方法。
         * 初始化CoordinatorPeerFinder对象，根据是否为单节点发现模式设置相应的主机解析器。
         *
         * @param settings 系统设置。
         * @param transportService 传输服务。
         * @param transportAddressConnector 传输地址连接器。
         * @param configuredHostsResolver 配置的主机解析器。
         */
        CoordinatorPeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                              ConfiguredHostsResolver configuredHostsResolver) {
            super(settings, transportService, transportAddressConnector,
                singleNodeDiscovery ? hostsResolver -> Collections.emptyList() : configuredHostsResolver);
        }

        /**
         * 当发现活动主节点时调用。
         * 同步块中，确保当前任期至少与发现的主节点任期一致，然后发送加入请求。
         *
         * @param masterNode 发现的主节点。
         * @param term 主节点的任期。
         */
        @Override
        protected void onActiveMasterFound(DiscoveryNode masterNode, long term) {
            synchronized (mutex) {
                ensureTermAtLeast(masterNode, term);
                joinHelper.sendJoinRequest(masterNode, joinWithDestination(lastJoin, masterNode, term));
            }
        }

        /**
         * 开始探测指定传输地址的对等节点。
         * 如果不是单节点发现模式，则调用父类的startProbe方法。
         *
         * @param transportAddress 传输地址。
         */
        @Override
        protected void startProbe(TransportAddress transportAddress) {
            if (singleNodeDiscovery == false) {
                super.startProbe(transportAddress);
            }
        }

        /**
         * 调用当发现的对等节点列表更新时。
         * 同步块中，根据当前模式和发现的节点列表决定是否启动选举调度器或关闭。
         */
        @Override
        protected void onFoundPeersUpdated() {
            synchronized (mutex) {
                final Iterable<DiscoveryNode> foundPeers = getFoundPeers();
                if (mode == Mode.CANDIDATE) {
                    final CoordinationState.VoteCollection expectedVotes = new CoordinationState.VoteCollection();
                    for (DiscoveryNode node : foundPeers) {
                        expectedVotes.addVote(node);
                    }
                    expectedVotes.addVote(Coordinator.this.getLocalNode());
                    final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
                    final boolean foundQuorum = CoordinationState.isElectionQuorum(expectedVotes, lastAcceptedState);

                    if (foundQuorum) {
                        if (electionScheduler == null) {
                            startElectionScheduler();
                        }
                    } else {
                        closePrevotingAndElectionScheduler();
                    }
                }
            }

            clusterBootstrapService.onFoundPeersUpdated();
        }
    }

    /**
     * 启动选举调度器。
     * 此方法用于初始化选举过程，安排预投票轮次。
     */
    private void startElectionScheduler() {
        // 断言electionScheduler当前为null，确保选举调度器只启动一次。
        assert electionScheduler == null : electionScheduler;

        // 如果本地节点不是主节点，则不启动选举调度器。
        if (getLocalNode().isMasterNode() == false) {
            return;
        }

        // 设置宽限期为0，即不等待，立即开始选举。
        final TimeValue gracePeriod = TimeValue.ZERO; // TODO 可变宽限期
        // 使用选举调度器工厂创建并启动一个新的选举调度器。
        electionScheduler = electionSchedulerFactory.startElectionScheduler(gracePeriod, new Runnable() {
            @Override
            public void run() {
                // 进入同步块以确保线程安全。
                synchronized (mutex) {
                    // 如果当前模式为候选人，则继续执行。
                    if (mode == Mode.CANDIDATE) {
                        // 获取最后接受的集群状态。
                        final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();

                        // 如果本地节点不是选举法定人数的一部分，则跳过预投票。
                        if (electionQuorumContainsLocalNode(lastAcceptedState) == false) {
                            logger.trace("skip prevoting as local node is not part of election quorum: {}",
                                lastAcceptedState.coordinationMetaData());
                            return;
                        }

                        // 如果已经存在预投票轮次，则关闭它。
                        if (prevotingRound != null) {
                            prevotingRound.close();
                        }
                        // 过滤并获取已发现的非Zen1节点列表。
                        final List<DiscoveryNode> discoveredNodes
                            = getDiscoveredNodes().stream().filter(n -> isZen1Node(n) == false).collect(Collectors.toList());

                        // 启动新的预投票轮次。
                        prevotingRound = preVoteCollector.start(lastAcceptedState, discoveredNodes);
                    }
                }
            }

            @Override
            public String toString() {
                // 提供Runnable的字符串表示，用于日志记录或调试。
                return "scheduling of new prevoting round";
            }
        });
    }

    /**
     * 获取已发现的节点集合。
     * 此方法返回由PeerFinder找到的节点集合。
     *
     * @return 返回一个可迭代的节点集合。
     */
    public Iterable<DiscoveryNode> getFoundPeers() {
        // 直接返回peerFinder中找到的节点集合。
        // TODO: 每个人都会获取这个集合然后添加本地节点，或许可以直接在这里添加本地节点？
        return peerFinder.getFoundPeers();
    }

    /**
     * 取消当前已提交的发布（如果存在）。
     * 此方法仅供测试使用。如果当前存在已提交的发布，将取消它。
     *
     * @return 如果发布被取消返回true，如果没有当前已提交的发布则返回false。
     */
    boolean cancelCommittedPublication() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 检查是否存在当前发布且该发布已提交。
            if (currentPublication.isPresent() && currentPublication.get().isCommitted()) {
                // 取消发布，并提供取消的原因。
                currentPublication.get().cancel("cancelCommittedPublication");
                return true;
            }
            // 如果没有当前发布或发布未提交，返回false。
            return false;
        }
    }

    class CoordinatorPublication extends Publication {

        // 发布请求，包含要发布的集群状态信息。
        private final PublishRequest publishRequest;
        // 本地节点确认事件的ListenableFuture。
        private final ListenableFuture<Void> localNodeAckEvent;
        // 确认监听器。
        private final AckListener ackListener;
        // 发布操作的ActionListener。
        private final ActionListener<Void> publishListener;
        // 发布上下文。
        private final PublicationTransportHandler.PublicationContext publicationContext;
        // 计划的可取消任务。
        private final Scheduler.ScheduledCancellable scheduledCancellable;

        // 存储在当前节点接受自身状态之前收到的其他节点的加入请求。
        private final List<Join> receivedJoins = new ArrayList<>();
        // 标记是否已处理收到的加入请求。
        private boolean receivedJoinsProcessed;

        /**
         * CoordinatorPublication类的构造方法。
         * 初始化协调发布所需的各种参数和监听器。
         *
         * @param publishRequest 要发布的请求对象，包含集群状态信息。
         * @param publicationContext 发布传输处理器的上下文，提供发布操作所需的传输相关上下文信息。
         * @param localNodeAckEvent 本地节点确认事件的ListenableFuture对象，用于处理本地节点确认。
         * @param ackListener 确认监听器，当集群中的节点确认或拒绝发布请求时，此监听器会被调用。
         * @param publishListener 发布操作的ActionListener，当发布操作完成或失败时，此监听器会被通知。
         */
        CoordinatorPublication(PublishRequest publishRequest, PublicationTransportHandler.PublicationContext publicationContext,
                               ListenableFuture<Void> localNodeAckEvent, AckListener ackListener, ActionListener<Void> publishListener) {
            // 调用父类构造方法，传入发布请求、自定义的AckListener和当前时间的毫秒数。
            super(publishRequest,
                new AckListener() {
                    // 覆盖onCommit方法，将提交时间传递给外部的ackListener。
                    @Override
                    public void onCommit(TimeValue commitTime) {
                        ackListener.onCommit(commitTime);
                    }

                    // 覆盖onNodeAck方法，处理节点确认逻辑。
                    @Override
                    public void onNodeAck(DiscoveryNode node, Exception e) {
                        // 对本地节点的确认进行特殊处理。
                        if (node.equals(getLocalNode())) {
                            synchronized (mutex) {
                                if (e == null) {
                                    localNodeAckEvent.onResponse(null); // 本地节点确认成功。
                                } else {
                                    localNodeAckEvent.onFailure(e); // 本地节点确认失败。
                                }
                            }
                        } else {
                            // 非本地节点的确认逻辑。
                            ackListener.onNodeAck(node, e);
                            if (e == null) {
                                // 如果确认成功，更新节点的已应用版本。
                                lagDetector.setAppliedVersion(node, publishRequest.getAcceptedState().version());
                            }
                        }
                    }
                },
                transportService.getThreadPool()::relativeTimeInMillis);

            // 初始化成员变量。
            this.publishRequest = publishRequest;
            this.publicationContext = publicationContext;
            this.localNodeAckEvent = localNodeAckEvent;
            this.ackListener = ackListener;
            this.publishListener = publishListener;
            // 计划发布超时任务。
            this.scheduledCancellable = transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        // 如果超时，取消发布。
                        cancel("timed out after " + publishTimeout);
                    }
                }

                @Override
                public String toString() {
                    return "scheduled timeout for " + this;
                }
            }, publishTimeout, Names.GENERIC);
        }

        /**
         * 移除当前的发布操作，并在适当的情况下将节点状态设置为候选人。
         * 此方法在发布操作结束但不成功时调用，例如在发布超时或失败的情况下。
         *
         * @param reason 导致发布操作结束的原因。
         */
        private void removePublicationAndPossiblyBecomeCandidate(String reason) {
            // 断言当前线程持有协调器的互斥锁。
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            // 断言currentPublication指向当前publication对象。
            assert currentPublication.get() == this;
            // 将currentPublication置为空，移除当前发布操作。
            currentPublication = Optional.empty();
            // 记录发布操作结束的日志。
            logger.debug("publication ended unsuccessfully: {}", this);

            // 检查当前节点是否已经通过提升任期来切换模式。
            // 如果当前发布仍然可以影响当前领导者的模式，则可能需要将节点变为候选人状态。
            if (isActiveForCurrentLeader()) {
                becomeCandidate(reason);
            }
        }

        /**
         * 检查当前发布是否仍然可以影响当前领导者的模式。
         * 如果当前节点是领导者并且发布的任期与当前任期匹配，则返回true。
         *
         * @return 如果当前发布可以影响当前领导者的模式，则返回true。
         */
        boolean isActiveForCurrentLeader() {
            // 如果当前模式是领导者，并且发布请求中接受的任期与当前任期相同，则返回true。
            return mode == Mode.LEADER && publishRequest.getAcceptedState().term() == getCurrentTerm();
        }
        /**
         * 当发布操作完成时调用，无论是成功还是失败。
         * 此方法处理发布操作的最终结果，包括本地节点的确认、集群状态的应用以及后续的逻辑。
         *
         * @param committed 表示发布操作是否已提交。
         */
        @Override
        protected void onCompletion(boolean committed) {
            // 断言当前线程持有协调器的互斥锁。
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            // 为本地节点确认事件添加监听器。
            localNodeAckEvent.addListener(new ActionListener<Void>() {
                @Override
                public void onResponse(Void ignore) {
                    // 断言当前线程持有协调器的互斥锁。
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    // 断言发布操作已提交。
                    assert committed;

                    // 处理所有收到的加入请求。
                    receivedJoins.forEach(CoordinatorPublication.this::handleAssociatedJoin);
                    // 断言收到的加入请求尚未处理。
                    assert receivedJoinsProcessed == false;
                    // 标记收到的加入请求已处理。
                    receivedJoinsProcessed = true;

                    // 通知集群应用器应用新的集群状态。
                    clusterApplier.onNewClusterState(CoordinatorPublication.this.toString(), () -> applierState,
                        new ClusterApplyListener() {
                            // 集群状态应用失败的回调。
                            @Override
                            public void onFailure(String source, Exception e) {
                                synchronized (mutex) {
                                    // 移除发布并可能变为候选人。
                                    removePublicationAndPossiblyBecomeCandidate("clusterApplier#onNewClusterState");
                                }
                                scheduledCancellable.cancel();
                                ackListener.onNodeAck(getLocalNode(), e);
                                publishListener.onFailure(e);
                            }

                            // 集群状态应用成功的回调。
                            @Override
                            public void onSuccess(String source) {
                                synchronized (mutex) {
                                    // 断言currentPublication指向当前publication对象。
                                    assert currentPublication.get() == CoordinatorPublication.this;
                                    // 将currentPublication置为空。
                                    currentPublication = Optional.empty();
                                    // 记录发布操作成功的日志。
                                    logger.debug("publication ended successfully: {}", CoordinatorPublication.this);
                                    // 如果在发布过程中发现新的任期，则更新观察到的最大任期。
                                    updateMaxTermSeen(getCurrentTerm());

                                    if (mode == Mode.LEADER) {
                                        final ClusterState state = getLastAcceptedState(); // 提交的状态
                                        if (electionQuorumContainsLocalNode(state) == false) {
                                            // 处理没有本地节点的选举法定人数的情况。
                                            final List<DiscoveryNode> masterCandidates = completedNodes().stream()
                                                .filter(DiscoveryNode::isMasterNode)
                                                .filter(node -> electionQuorumContains(state, node))
                                                .collect(Collectors.toList());
                                            if (masterCandidates.isEmpty() == false) {
                                                // 退位给随机选出的主节点候选人。
                                                abdicateTo(masterCandidates.get(random.nextInt(masterCandidates.size())));
                                            }
                                        } else {
                                            // 如果本地节点在选举法定人数中，根据需要安排重配置。
                                            scheduleReconfigurationIfNeeded();
                                        }
                                    }
                                    // 开始延迟检测器。
                                    lagDetector.startLagDetector(publishRequest.getAcceptedState().version());
                                }
                                scheduledCancellable.cancel();
                                ackListener.onNodeAck(getLocalNode(), null);
                                publishListener.onResponse(null);
                            }
                        });
                }

                // 本地节点确认事件失败的回调。
                @Override
                public void onFailure(Exception e) {
                    // 断言当前线程持有协调器的互斥锁。
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    // 移除发布并可能变为候选人。
                    removePublicationAndPossiblyBecomeCandidate("Publication.onCompletion(false)");
                    scheduledCancellable.cancel();

                    // 创建发布失败的异常。
                    final FailedToCommitClusterStateException exception =
                        new FailedToCommitClusterStateException("publication failed", e);
                    // 通知监听器本地节点确认失败。
                    ackListener.onNodeAck(getLocalNode(), exception); // 其他节点已确认，但主节点未确认。
                    // 通知发布操作失败。
                    publishListener.onFailure(exception);
                }
            }, EsExecutors.newDirectExecutorService(), transportService.getThreadContext());
        }

        /**
         * 处理与当前发布相关的节点加入请求。
         * 如果收到的加入请求的任期与当前任期相同，并且当前节点尚未记录来自该节点的加入投票，
         * 则处理该加入请求。
         *
         * @param join 要处理的节点加入请求。
         */
        private void handleAssociatedJoin(Join join) {
            // 检查加入请求的任期是否与当前任期相同，并且当前节点没有该节点的加入投票。
            if (join.getTerm() == getCurrentTerm() && hasJoinVoteFrom(join.getSourceNode()) == false) {
                logger.trace("handling {}", join); // 记录处理加入请求的日志。
                handleJoin(join); // 调用handleJoin方法处理加入请求。
            }
        }

        /**
         * 检查是否有足够的投票来提交发布。
         * 根据协调状态对象的投票集合判断是否满足发布法定人数。
         *
         * @param votes 协调状态的投票集合。
         * @return 如果满足发布法定人数，则返回true。
         */
        @Override
        protected boolean isPublishQuorum(CoordinationState.VoteCollection votes) {
            // 断言当前线程持有协调器的互斥锁。
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            // 返回协调状态对象的isPublishQuorum方法的结果。
            return coordinationState.get().isPublishQuorum(votes);
        }

        /**
         * 处理来自其他节点的发布响应。
         * 根据源节点和发布响应调用协调状态对象的handlePublishResponse方法，
         * 并返回处理结果，可能包含应用提交请求或为空。
         *
         * @param sourceNode 响应发布请求的源节点。
         * @param publishResponse 从源节点接收到的发布响应。
         * @return 处理结果，可能是包含ApplyCommitRequest的Optional，或者为空。
         */
        @Override
        protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode,
                                                                     PublishResponse publishResponse) {
            // 断言当前线程持有协调器的互斥锁。
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            // 断言当前任期不小于发布响应中的任期。
            assert getCurrentTerm() >= publishResponse.getTerm();
            // 调用协调状态对象的handlePublishResponse方法，并返回结果。
            return coordinationState.get().handlePublishResponse(sourceNode, publishResponse);
        }
        /**
         * 当收到节点加入请求时调用。
         * 此方法处理来自其他节点的加入请求，并根据当前状态决定是立即处理还是稍后处理。
         *
         * @param join 收到的节点加入请求。
         */
        @Override
        protected void onJoin(Join join) {
            // 断言当前线程持有协调器的互斥锁。
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            if (receivedJoinsProcessed) {
                // 如果已处理过收到的加入请求，说明这是一个迟到的响应，需要立即处理。
                handleAssociatedJoin(join);
            } else {
                // 否则，将加入请求添加到列表中，稍后处理。
                receivedJoins.add(join);
            }
        }

        /**
         * 当缺少节点加入投票时调用。
         * 如果远程节点没有在其发布响应中包含加入投票，可能需要提升当前任期以获得该节点的投票。
         *
         * @param discoveryNode 没有包含加入投票的远程节点。
         */
        @Override
        protected void onMissingJoin(DiscoveryNode discoveryNode) {
            // 断言当前线程持有协调器的互斥锁。
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            // 如果没有从该节点收到加入投票，并且当前节点没有记录该节点的投票，则提升任期。
            if (hasJoinVoteFrom(discoveryNode) == false) {
                final long term = publishRequest.getAcceptedState().term();
                logger.debug("onMissingJoin: no join vote from {}, bumping term to exceed {}", discoveryNode, term);
                updateMaxTermSeen(term + 1); // 更新观察到的最大任期，以确保获得该节点的投票。
            }
        }

        /**
         * 发送发布请求到指定目的地节点。
         * 此方法封装了发布请求的发送逻辑，并确保响应在协调器的线程上下文中处理。
         *
         * @param destination 目标节点。
         * @param publishRequest 要发送的发布请求。
         * @param responseActionListener 发布响应的ActionListener。
         */
        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<PublishWithJoinResponse> responseActionListener) {
            // 使用publicationContext发送发布请求，并包装响应监听器以确保线程安全。
            publicationContext.sendPublishRequest(destination, publishRequest, wrapWithMutex(responseActionListener));
        }

        /**
         * 发送应用提交请求到指定目的地节点。
         * 此方法封装了应用提交请求的发送逻辑，并确保响应在协调器的线程上下文中处理。
         *
         * @param destination 目标节点。
         * @param applyCommit 要发送的应用提交请求。
         * @param responseActionListener 应用提交响应的ActionListener。
         */
        @Override
        protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                       ActionListener<Empty> responseActionListener) {
            // 使用publicationContext发送应用提交请求，并包装响应监听器以确保线程安全。
            publicationContext.sendApplyCommit(destination, applyCommit, wrapWithMutex(responseActionListener));
        }
    }
    /**
     * 为设置构建器添加Zen1属性。
     * 此方法临时用于BWC开发，一旦完成就应移除。
     *
     * @param isZen1Node 指示节点是否为Zen1节点的布尔值。
     * @param builder    设置构建器。
     * @return 更新后的设置构建器。
     */
    public static Settings.Builder addZen1Attribute(boolean isZen1Node, Settings.Builder builder) {
        // 在设置构建器中添加一个属性，表示节点是否为Zen1节点。
        return builder.put("node.attr.zen1", isZen1Node);
    }

    /**
     * 判断节点是否为Zen1节点。
     * 此方法临时用于BWC开发，一旦完成就应移除。
     *
     * @param discoveryNode 要检查的发现节点。
     * @return 如果节点是Zen1节点，则返回true。
     */
    public static boolean isZen1Node(DiscoveryNode discoveryNode) {
        // 检查节点的版本是否早于7.0.0，或者是节点属性中包含"zen1"且值为true。
        return discoveryNode.getVersion().before(Version.V_7_0_0) ||
            (Booleans.isTrue(discoveryNode.getAttributes().getOrDefault("zen1", "false")));
    }
}
