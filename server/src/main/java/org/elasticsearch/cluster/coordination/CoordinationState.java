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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.cluster.coordination.Coordinator.ZEN1_BWC_TERM;

/**
 * 集群状态协调算法的核心
 */
public class CoordinationState {

    // 日志记录器，用于记录CoordinationState类的日志信息
    private static final Logger logger = LogManager.getLogger(CoordinationState.class);

    // 当前节点的引用
    private final DiscoveryNode localNode;

    // 持久化状态，存储需要在集群节点重启后保留的状态信息
    private final PersistedState persistedState;

    // 临时状态，以下变量在节点重启后可能会丢失
    private VoteCollection joinVotes; // 用于收集加入集群时其他节点的投票
    private boolean startedJoinSinceLastReboot; // 表示自上次重启以来是否已经开始加入集群
    private boolean electionWon; // 表示当前节点是否赢得了选举
    private long lastPublishedVersion; // 记录最后发布的状态版本
    private VotingConfiguration lastPublishedConfiguration; // 记录最后发布的投票配置
    private VoteCollection publishVotes; // 用于收集发布状态时的投票

    public CoordinationState(Settings settings, DiscoveryNode localNode, PersistedState persistedState) {
        this.localNode = localNode;

        // persisted state
        this.persistedState = persistedState;

        // transient state
        this.joinVotes = new VoteCollection();
        this.startedJoinSinceLastReboot = false;
        this.electionWon = false;
        this.lastPublishedVersion = 0L;
        this.lastPublishedConfiguration = persistedState.getLastAcceptedState().getLastAcceptedConfiguration();
        this.publishVotes = new VoteCollection();
    }

    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }

    public ClusterState getLastAcceptedState() {
        return persistedState.getLastAcceptedState();
    }

    public long getLastAcceptedTerm() {
        return getLastAcceptedState().term();
    }

    public long getLastAcceptedVersion() {
        return getLastAcceptedState().version();
    }

    private long getLastAcceptedVersionOrMetaDataVersion() {
        return getLastAcceptedState().getVersionOrMetaDataVersion();
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return getLastAcceptedState().getLastCommittedConfiguration();
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return getLastAcceptedState().getLastAcceptedConfiguration();
    }

    public long getLastPublishedVersion() {
        return lastPublishedVersion;
    }

    public boolean electionWon() {
        return electionWon;
    }

    public boolean isElectionQuorum(VoteCollection votes) {
        return isElectionQuorum(votes, getLastAcceptedState());
    }

    static boolean isElectionQuorum(VoteCollection votes, ClusterState lastAcceptedState) {
        return votes.isQuorum(lastAcceptedState.getLastCommittedConfiguration())
            && votes.isQuorum(lastAcceptedState.getLastAcceptedConfiguration());
    }

    public boolean isPublishQuorum(VoteCollection votes) {
        return votes.isQuorum(getLastCommittedConfiguration()) && votes.isQuorum(lastPublishedConfiguration);
    }

    public boolean containsJoinVoteFor(DiscoveryNode node) {
        return joinVotes.containsVoteFor(node);
    }

    public boolean joinVotesHaveQuorumFor(VotingConfiguration votingConfiguration) {
        return joinVotes.isQuorum(votingConfiguration);
    }

    /**
     * 用于通过注入初始状态和配置来引导集群启动。
     *
     * @param initialState 要使用的初始状态。必须具有术语0，版本等于最后接受的版本，并且配置非空。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public void setInitialState(ClusterState initialState) {
        // 获取最后接受的配置
        final VotingConfiguration lastAcceptedConfiguration = getLastAcceptedConfiguration();
        // 如果最后接受的配置非空，则拒绝设置初始状态
        if (lastAcceptedConfiguration.isEmpty() == false) {
            logger.debug("setInitialState: rejecting since last-accepted configuration is nonempty: {}", lastAcceptedConfiguration);
            throw new CoordinationStateRejectedException(
                "initial state already set: last-accepted configuration now " + lastAcceptedConfiguration);
        }

        // 断言当前接受的术语为0
        assert getLastAcceptedTerm() == 0 : getLastAcceptedTerm();
        // 断言最后提交的配置为空
        assert getLastCommittedConfiguration().isEmpty() : getLastCommittedConfiguration();
        // 断言最后发布版本为0
        assert lastPublishedVersion == 0 : lastPublishedVersion;
        // 断言最后发布配置为空
        assert lastPublishedConfiguration.isEmpty() : lastPublishedConfiguration;
        // 断言选举未胜利
        assert electionWon == false;
        // 断言加入投票集合为空
        assert joinVotes.isEmpty() : joinVotes;
        assert publishVotes.isEmpty() : publishVotes;

        // 断言初始状态的术语为0
        assert initialState.term() == 0 : initialState + " should have term 0";
        // 断言初始状态的版本等于最后接受的版本
        assert initialState.version() == getLastAcceptedVersion() : initialState + " should have version " + getLastAcceptedVersion();
        // 断言初始状态的最后接受配置非空
        assert initialState.getLastAcceptedConfiguration().isEmpty() == false;
        // 断言初始状态的最后提交配置非空
        assert initialState.getLastCommittedConfiguration().isEmpty() == false;

        // 设置持久化状态为最后接受的状态
        persistedState.setLastAcceptedState(initialState);
    }

    /**
     * 可以随时安全调用，以将此实例移动到新的术语。
     *
     * @param startJoinRequest 指定请求加入的节点的开始加入请求。
     * @return 应该发送到加入目标节点的加入对象。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public Join handleStartJoin(StartJoinRequest startJoinRequest) {
        // 如果提供的术语不大于当前术语，则忽略请求
        if (startJoinRequest.getTerm() <= getCurrentTerm()) {
            logger.debug("handleStartJoin: ignoring [{}] as term provided is not greater than current term [{}]",
                startJoinRequest, getCurrentTerm());
            throw new CoordinationStateRejectedException("incoming term " + startJoinRequest.getTerm() +
                " not greater than current term " + getCurrentTerm());
        }

        // 记录日志，表示由于开始加入请求而离开当前术语
        logger.debug("handleStartJoin: leaving term [{}] due to {}", getCurrentTerm(), startJoinRequest);

        // 如果存在非空的加入投票，则记录日志并丢弃它们
        if (joinVotes.isEmpty() == false) {
            // ...
            logger.debug("handleStartJoin: discarding {}: {}", joinVotes, reason);
        }

        // 设置持久化状态的当前术语
        persistedState.setCurrentTerm(startJoinRequest.getTerm());
        // 断言当前术语等于开始加入请求的术语
        assert getCurrentTerm() == startJoinRequest.getTerm();
        // 重置最后发布版本
        lastPublishedVersion = 0;
        // 更新最后发布配置为最后接受的配置
        lastPublishedConfiguration = getLastAcceptedConfiguration();
        // 标记自上次重启以来已开始加入
        startedJoinSinceLastReboot = true;
        // 重置选举胜利状态
        electionWon = false;
        // 重置加入投票集合
        joinVotes = new VoteCollection();
        // 重置发布投票集合
        publishVotes = new VoteCollection();

        // 创建并返回一个新的加入对象
        return new Join(localNode, startJoinRequest.getSourceNode(), getCurrentTerm(), getLastAcceptedTerm(),
            getLastAcceptedVersionOrMetaDataVersion());
    }

    /**
     * 在收到加入请求（Join）时调用。
     *
     * @param join 收到的加入请求。
     * @return 如果此实例尚未为此术语自给定源节点收到加入投票，则返回true。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public boolean handleJoin(Join join) {
        // 断言处理的加入请求是针对本地节点的
        assert join.targetMatches(localNode) : "handling join " + join + " for the wrong node " + localNode;
        // 检查请求的任期是否与当前任期匹配
        if (join.getTerm() != getCurrentTerm()) {
            logger.debug("handleJoin: ignored join due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), join.getTerm());
            throw new CoordinationStateRejectedException(
                "incoming term " + join.getTerm() + " does not match current term " + getCurrentTerm());
        }
        // 如果自上次重启以来任期尚未增加，则忽略加入请求
        if (startedJoinSinceLastReboot == false) {
            logger.debug("handleJoin: ignored join as term was not incremented yet after reboot");
            throw new CoordinationStateRejectedException("ignored join as term has not been incremented yet after reboot");
        }
        // 获取最后接受的任期
        final long lastAcceptedTerm = getLastAcceptedTerm();
        // 如果加入者的最后接受任期大于当前最后接受任期，则忽略加入请求
        if (join.getLastAcceptedTerm() > lastAcceptedTerm) {
            logger.debug("handleJoin: ignored join as joiner has a better last accepted term (expected: <=[{}], actual: [{}])",
                lastAcceptedTerm, join.getLastAcceptedTerm());
            throw new CoordinationStateRejectedException("incoming last accepted term " + join.getLastAcceptedTerm() +
                " of join higher than current last accepted term " + lastAcceptedTerm);
        }
        // 如果加入者的最后接受任期与当前相同，但版本号大于最后接受的版本号，则忽略加入请求
        if (join.getLastAcceptedTerm() == lastAcceptedTerm && join.getLastAcceptedVersion() > getLastAcceptedVersionOrMetaDataVersion()) {
            logger.debug(
                "handleJoin: ignored join as joiner has a better last accepted version (expected: <=[{}], actual: [{}]) in term {}",
                getLastAcceptedVersionOrMetaDataVersion(), join.getLastAcceptedVersion(), lastAcceptedTerm);
            throw new CoordinationStateRejectedException("incoming last accepted version " + join.getLastAcceptedVersion() +
                " of join higher than current last accepted version " + getLastAcceptedVersionOrMetaDataVersion()
                + " in term " + lastAcceptedTerm);
        }
        // 如果尚未接收到初始配置，则拒绝加入请求
        if (getLastAcceptedConfiguration().isEmpty()) {
            // We do not check for an election won on setting the initial configuration, so it would be possible to end up in a state where
            // we have enough join votes to have won the election immediately on setting the initial configuration. It'd be quite
            // complicated to restore all the appropriate invariants when setting the initial configuration (it's not just electionWon)
            // so instead we just reject join votes received prior to receiving the initial configuration.
            logger.debug("handleJoin: rejecting join since this node has not received its initial configuration yet");
            throw new CoordinationStateRejectedException("rejecting join since this node has not received its initial configuration yet");
        }


        // 为加入请求的源节点添加投票
        boolean added = joinVotes.addVote(join.getSourceNode());
        // 记录选举胜利状态
        boolean prevElectionWon = electionWon;
        electionWon = isElectionQuorum(joinVotes);
        // 断言选举胜利状态不能从未胜利变为胜利
        assert !prevElectionWon || electionWon;
        // 记录加入请求处理的日志
        logger.debug("handleJoin: added join {} from [{}] for election, electionWon={} lastAcceptedTerm={} lastAcceptedVersion={}",
            join, join.getSourceNode(), electionWon, lastAcceptedTerm, getLastAcceptedVersion());

        // 如果选举胜利且之前未胜利，则更新最后发布版本
        if (electionWon && prevElectionWon == false) {
            logger.debug("handleJoin: election won in term [{}] with {}", getCurrentTerm(), joinVotes);
            lastPublishedVersion = getLastAcceptedVersion();
        }
        return added; // 返回是否成功添加了投票
    }


    /**
     * 用于准备发布给定的集群状态。
     *
     * @param clusterState 要发布的集群状态。
     * @return 一个 PublishRequest，用于发布给定的集群状态。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public PublishRequest handleClientValue(ClusterState clusterState) {
        // 如果选举未胜利，则忽略请求并抛出异常
        if (electionWon == false) {
            logger.debug("handleClientValue: ignored request as election not won");
            throw new CoordinationStateRejectedException("election not won");
        }
        // 如果尚未接受上一个值，则不能开始发布下一个值
        if (lastPublishedVersion != getLastAcceptedVersion()) {
            logger.debug("handleClientValue: cannot start publishing next value before accepting previous one");
            throw new CoordinationStateRejectedException("cannot start publishing next value before accepting previous one");
        }
        // 检查请求的任期是否与当前任期匹配
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handleClientValue: ignored request due to term mismatch " +
                    "(expected: [term {} version >{}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }
        // 检查请求的版本是否大于最后发布的版本
        if (clusterState.version() <= lastPublishedVersion) {
            logger.debug("handleClientValue: ignored request due to version mismatch " +
                    "(expected: [term {} version >{}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming cluster state version " + clusterState.version() +
                " lower or equal to last published version " + lastPublishedVersion);
        }

        // 如果当前集群状态的配置与最后接受的配置不同，并且最后提交的配置也不等于最后接受的配置，则抛出异常
        if (clusterState.getLastAcceptedConfiguration().equals(getLastAcceptedConfiguration()) == false
            && getLastCommittedConfiguration().equals(getLastAcceptedConfiguration()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration while not already reconfiguring");
            throw new CoordinationStateRejectedException("only allow reconfiguration while not already reconfiguring");
        }
        // 如果新配置没有获得加入投票的法定人数，则抛出异常
        if (joinVotesHaveQuorumFor(clusterState.getLastAcceptedConfiguration()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration if joinVotes have quorum for new config");
            throw new CoordinationStateRejectedException("only allow reconfiguration if joinVotes have quorum for new config");
        }

        // 断言最后提交的配置与当前的配置相同
        assert clusterState.getLastCommittedConfiguration().equals(getLastCommittedConfiguration()) :
            "last committed configuration should not change";

        // 更新最后发布的版本和配置
        lastPublishedVersion = clusterState.version();
        lastPublishedConfiguration = clusterState.getLastAcceptedConfiguration();
        // 重置发布投票集合
        publishVotes = new VoteCollection();

        logger.trace("handleClientValue: processing request for version [{}] and term [{}]", lastPublishedVersion, getCurrentTerm());

        // 返回一个用于发布请求的PublishRequest
        return new PublishRequest(clusterState);
    }

    /**
     * 在收到PublishRequest时调用。
     *
     * @param publishRequest 收到的发布请求。
     * @return 发布响应，可以将其发送回PublishRequest的发送者。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public PublishResponse handlePublishRequest(PublishRequest publishRequest) {
        final ClusterState clusterState = publishRequest.getAcceptedState(); // 获取发布请求中接受的集群状态
        // 检查请求的任期是否与当前任期匹配
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handlePublishRequest: ignored publish request due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), clusterState.term());
            throw new CoordinationStateRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }
        // 检查请求的任期是否与最后接受的任期相同且版本是否大于最后接受的版本
        if (clusterState.term() == getLastAcceptedTerm() && clusterState.version() <= getLastAcceptedVersion()) {
            if (clusterState.term() == ZEN1_BWC_TERM
                && clusterState.nodes().getMasterNode().equals(getLastAcceptedState().nodes().getMasterNode()) == false) {
                logger.debug("handling publish request in compatibility mode despite version mismatch (expected: >[{}], actual: [{}])",
                    getLastAcceptedVersion(), clusterState.version());
            } else {
                logger.debug("handlePublishRequest: ignored publish request due to version mismatch (expected: >[{}], actual: [{}])",
                    getLastAcceptedVersion(), clusterState.version());
                throw new CoordinationStateRejectedException("incoming version " + clusterState.version()
                    + " lower or equal to current version " + getLastAcceptedVersion());
            }
        }

        logger.trace("handlePublishRequest: accepting publish request for version [{}] and term [{}]",
            clusterState.version(), clusterState.term());
        persistedState.setLastAcceptedState(clusterState); // 设置持久化状态为最后接受的集群状态
        assert getLastAcceptedState() == clusterState; // 断言最后接受的集群状态与请求中的集群状态相同

        return new PublishResponse(clusterState.term(), clusterState.version()); // 返回发布响应
    }

    /**
     * 在收到来自给定sourceNode的PublishResponse时调用。
     *
     * @param sourceNode       PublishResponse的发送者。
     * @param publishResponse 收到的发布响应。
     * @return 一个可选的ApplyCommitRequest，如果存在，可以广播给所有对等节点，表示此发布已在对等节点的法定人数中被接受，因此已提交。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
        // 如果选举未胜利，则忽略响应并抛出异常
        if (electionWon == false) {
            logger.debug("handlePublishResponse: ignored response as election not won");
            throw new CoordinationStateRejectedException("election not won");
        }
        // 检查响应的任期是否与当前任期匹配
        if (publishResponse.getTerm() != getCurrentTerm()) {
            logger.debug("handlePublishResponse: ignored publish response due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), publishResponse.getTerm());
            throw new CoordinationStateRejectedException("incoming term " + publishResponse.getTerm()
                + " does not match current term " + getCurrentTerm());
        }
        // 检查响应的版本是否与最后发布版本匹配
        if (publishResponse.getVersion() != lastPublishedVersion) {
            logger.debug("handlePublishResponse: ignored publish response due to version mismatch (expected: [{}], actual: [{}])",
                lastPublishedVersion, publishResponse.getVersion());
            throw new CoordinationStateRejectedException("incoming version " + publishResponse.getVersion()
                + " does not match current version " + lastPublishedVersion);
        }

        logger.trace("handlePublishResponse: accepted publish response for version [{}] and term [{}] from [{}]",
            publishResponse.getVersion(), publishResponse.getTerm(), sourceNode);
        publishVotes.addVote(sourceNode); // 为sourceNode添加投票
        // 如果发布投票达到法定人数
        if (isPublishQuorum(publishVotes)) {
            logger.trace("handlePublishResponse: value committed for version [{}] and term [{}]",
                publishResponse.getVersion(), publishResponse.getTerm());
            // 返回一个ApplyCommitRequest，表示此发布已被接受并可以提交
            return Optional.of(new ApplyCommitRequest(localNode, publishResponse.getTerm(), publishResponse.getVersion()));
        }

        return Optional.empty(); // 如果没有达到法定人数，则返回空
    }

    /**
     * 在收到ApplyCommitRequest时调用。相应地更新已提交的配置。
     *
     * @param applyCommit 收到的ApplyCommitRequest。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public void handleCommit(ApplyCommitRequest applyCommit) {
        // 检查请求的任期是否与当前任期匹配
        if (applyCommit.getTerm() != getCurrentTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getTerm(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming term " + applyCommit.getTerm() + " does not match current term " +
                getCurrentTerm());
        }
        // 检查请求的任期是否与最后接受的任期匹配
        if (applyCommit.getTerm() != getLastAcceptedTerm()) {
            // 记录日志并抛出异常
            // ...
            throw new CoordinationStateRejectedException("incoming term " + applyCommit.getTerm() + " does not match last accepted term " +
                getLastAcceptedTerm());
        }
        // 检查请求的版本是否与最后接受的版本匹配
        if (applyCommit.getVersion() != getLastAcceptedVersion()) {
            logger.debug("handleCommit: ignored commit request due to version mismatch (term {}, expected: [{}], actual: [{}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming version " + applyCommit.getVersion() +
                " does not match current version " + getLastAcceptedVersion());
        }

        logger.trace("handleCommit: applying commit request for term [{}] and version [{}]", applyCommit.getTerm(),
            applyPersistedState().getLastAcceptedVersion());

        // 标记最后接受的状态为已提交
        applyPersistedState().markLastAcceptedStateAsCommitted();
        // 断言最后提交的配置与最后接受的配置相等
        assert getLastCommittedConfiguration().equals(getLastAcceptedConfiguration());
    }

    /**
     * 检查状态的不变性。
     */
    public void invariant() {
        // 断言最后接受的任期不大于当前任期
        assert getLastAcceptedTerm() <= getCurrentTerm();
        // 断言选举胜利状态与加入投票达到法定人数状态相同
        assert electionWon() == isElectionQuorum(joinVotes);
        if (electionWon()) {
            // 如果选举胜利，断言最后发布的版本至少等于最后接受的版本
            assert getLastPublishedVersion() >= getLastAcceptedVersion();
        } else {
            // 如果选举没有胜利，断言最后发布的版本为0
            assert getLastPublishedVersion() == 0L;
        }
        // 断言如果选举胜利，则自上次重启以来已开始加入
        assert electionWon() == false || startedJoinSinceLastReboot;
        // 断言发布投票不为空或选举胜利
        assert publishVotes.isEmpty() || electionWon();
    }

    /**
     * 用于{@link CoordinationState}的可插拔持久化层。
     */
    public interface PersistedState {

        /**
         * 返回当前任期。
         * @return 当前任期。
         */
        long getCurrentTerm();

        /**
         * 返回最后接受的集群状态。
         * @return 最后接受的集群状态。
         */
        ClusterState getLastAcceptedState();

        /**
         * 设置新的当前任期。
         * 成功调用此方法后，{@link #getCurrentTerm()}应该返回最后设置的任期。
         * {@link #getLastAcceptedState()}返回的值不应受调用此方法的影响。
         * @param currentTerm 要设置的新任期。
         */
        void setCurrentTerm(long currentTerm);

        /**
         * 设置新的最后接受的集群状态。
         * 成功调用此方法后，{@link #getLastAcceptedState()}应该返回最后设置的集群状态。
         * {@link #getCurrentTerm()}返回的值不应受调用此方法的影响。
         * @param clusterState 要设置的新集群状态。
         */
        void setLastAcceptedState(ClusterState clusterState);

        /**
         * 将最后接受的集群状态标记为已提交。
         * 成功调用此方法后，{@link #getLastAcceptedState()}应该返回最后设置的集群状态，
         * 最后提交的配置现在应与最后接受的配置相对应，如果设置了集群UUID，则将其标记为已提交。
         */
        default void markLastAcceptedStateAsCommitted() {
            final ClusterState lastAcceptedState = getLastAcceptedState(); // 获取最后接受的集群状态
            MetaData.Builder metaDataBuilder = null; // 元数据构建器

            // 如果最后接受的配置与最后提交的配置不相等，则更新协调元数据
            if (lastAcceptedState.getLastAcceptedConfiguration().equals(lastAcceptedState.getLastCommittedConfiguration()) == false) {
                final CoordinationMetaData coordinationMetaData = CoordinationMetaData.builder(lastAcceptedState.coordinationMetaData())
                    .lastCommittedConfiguration(lastAcceptedState.getLastAcceptedConfiguration())
                    .build();
                metaDataBuilder = MetaData.builder(lastAcceptedState.metaData());
                metaDataBuilder.coordinationMetaData(coordinationMetaData);
            }

            // 检查集群UUID是否已知，如果不是，则断言集群处于Zen1兼容性任期
            assert lastAcceptedState.metaData().clusterUUID().equals(MetaData.UNKNOWN_CLUSTER_UUID) == false ||
                lastAcceptedState.term() == ZEN1_BWC_TERM :
                "received cluster state with empty cluster uuid but not Zen1 BWC term: " + lastAcceptedState;

            // 如果集群UUID已知但尚未标记为已提交，则将其标记为已提交
            if (lastAcceptedState.metaData().clusterUUID().equals(MetaData.UNKNOWN_CLUSTER_UUID) == false &&
                lastAcceptedState.metaData().clusterUUIDCommitted() == false) {
                if (metaDataBuilder == null) {
                    metaDataBuilder = MetaData.builder(lastAcceptedState.metaData());
                }
                metaDataBuilder.clusterUUIDCommitted(true);
                logger.info("cluster UUID set to [{}]", lastAcceptedState.metaData().clusterUUID());
            }

            // 如果更新了元数据，则使用新的元数据设置最后接受的集群状态
            if (metaDataBuilder != null) {
                setLastAcceptedState(ClusterState.builder(lastAcceptedState).metaData(metaDataBuilder).build());
            }
        }
    }

    public class VoteCollection {

        /**
         * 存储节点的映射，键为节点ID，值为节点对象。
         */
        private final Map<String, DiscoveryNode> nodes;

        /**
         * 为来自特定节点的投票添加到集合中。
         * @param sourceNode 投票的节点。
         * @return 如果集合中之前没有该节点的投票，则添加成功并返回true。
         */
        public boolean addVote(DiscoveryNode sourceNode) {
            return nodes.put(sourceNode.getId(), sourceNode) == null;
        }

        /**
         * 构造函数，初始化节点映射。
         */
        public VoteCollection() {
            nodes = new HashMap<>();
        }

        /**
         * 检查是否达到法定人数。
         * @param configuration 投票配置。
         * @return 如果根据投票配置达到法定人数，则返回true。
         */
        public boolean isQuorum(VotingConfiguration configuration) {
            return configuration.hasQuorum(nodes.keySet());
        }

        /**
         * 检查集合中是否包含特定节点的投票。
         * @param node 要检查的节点。
         * @return 如果集合包含该节点的投票，则返回true。
         */
        public boolean containsVoteFor(DiscoveryNode node) {
            return nodes.containsKey(node.getId());
        }

        /**
         * 检查投票集合是否为空。
         * @return 如果集合为空，则返回true。
         */
        public boolean isEmpty() {
            return nodes.isEmpty();
        }

        /**
         * 获取投票节点的集合。
         * @return 一个不可修改的节点集合视图。
         */
        public Collection<DiscoveryNode> nodes() {
            return Collections.unmodifiableCollection(nodes.values());
        }

        @Override
        public String toString() {
            // 返回节点ID列表的字符串表示。
            return "VoteCollection{" + String.join(",", nodes.keySet()) + "}";
        }

        @Override
        public boolean equals(Object o) {
            // 检查对象是否相等。
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VoteCollection that = (VoteCollection) o;

            return nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            // 返回此对象的哈希码，基于节点映射的哈希码。
            return nodes.hashCode();
        }
    }
}
