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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class JoinTaskExecutor implements ClusterStateTaskExecutor<JoinTaskExecutor.Task> {

    /**
     * 资源分配服务，用于管理集群中的资源分配。
     * <p>
     * AllocationService负责处理分片的分配逻辑，包括分片的启动、停止、重新分配等。
     */
    private final AllocationService allocationService;

    /**
     * 日志记录器，用于记录日志信息。
     * <p>
     * Logger是Apache Log4j 2中的一个类，用于不同级别的日志记录，如DEBUG、INFO、WARN、ERROR。
     */
    private final Logger logger;

    /**
     * 本地节点上设置的最小主节点数量。
     * <p>
     * minimumMasterNodesOnLocalNode定义了在当前节点上需要的最小主节点数量，以确保集群在脑裂或其他网络分区情况下的稳定性。
     * 这个值通常用于设置集群的高可用性，防止因网络分区导致无法进行主节点选举。
     */
    private final int minimumMasterNodesOnLocalNode;

    public static class Task {

        private final DiscoveryNode node;
        private final String reason;

        public Task(DiscoveryNode node, String reason) {
            this.node = node;
            this.reason = reason;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return node != null ? node + " " + reason : reason;
        }

        public boolean isBecomeMasterTask() {
            return reason.equals(BECOME_MASTER_TASK_REASON);
        }

        public boolean isFinishElectionTask() {
            return reason.equals(FINISH_ELECTION_TASK_REASON);
        }

        private static final String BECOME_MASTER_TASK_REASON = "_BECOME_MASTER_TASK_";
        private static final String FINISH_ELECTION_TASK_REASON = "_FINISH_ELECTION_";
    }

    public JoinTaskExecutor(Settings settings, AllocationService allocationService, Logger logger) {
        this.allocationService = allocationService;
        this.logger = logger;
        minimumMasterNodesOnLocalNode = ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(settings);
    }

    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> joiningNodes) throws Exception {
        final ClusterTasksResult.Builder<Task> results = ClusterTasksResult.builder();

        final DiscoveryNodes currentNodes = currentState.nodes();
        boolean nodesChanged = false; // 用于标记节点是否有变化
        ClusterState.Builder newState;

        // 如果只有一个加入节点，并且完成了选举任务，则直接返回成功
        if (joiningNodes.size() == 1 && joiningNodes.get(0).isFinishElectionTask()) {
            return results.successes(joiningNodes).build(currentState);
            // 如果当前没有主节点，并且有节点请求成为主节点
        } else if (currentNodes.getMasterNode() == null && joiningNodes.stream().anyMatch(Task::isBecomeMasterTask)) {
            assert joiningNodes.stream().anyMatch(Task::isFinishElectionTask)
                : "becoming a master but election is not finished " + joiningNodes;
            // 使用加入的节点尝试成为主节点
            newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
            nodesChanged = true;
        } else if (currentNodes.isLocalNodeElectedMaster() == false) {
            // 如果当前节点不是主节点，则抛出异常
            logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
            throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
        } else {
            // 如果当前节点是主节点，创建一个新的ClusterState.Builder实例
            newState = ClusterState.builder(currentState);
        }

        // 创建DiscoveryNodes.Builder用于构建新的节点信息
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

        // 断言当前节点是主节点
        assert nodesBuilder.isLocalNodeElectedMaster();

        // 获取集群的最小和最大节点版本
        Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
        Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
        // 确定是否强制执行主版本转换
        final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
        // 处理节点加入
        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // 如果是成为主节点或完成选举任务，则无操作
            } else if (currentNodes.nodeExists(joinTask.node())) {
                // 如果集群中已存在该节点，则记录日志
                logger.debug("received a join request for an existing node [{}]", joinTask.node());
            } else {
                // 处理新节点加入
                final DiscoveryNode node = joinTask.node();
                try {
                    if (enforceMajorVersion) {
                        // 确保新节点的主版本与集群兼容
                        ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                    }
                    // 确保新节点的版本与集群兼容
                    ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                    // 确保新节点支持集群中的所有索引
                    ensureIndexCompatibility(node.getVersion(), currentState.getMetaData());
                    // 将新节点添加到节点构建器中
                    nodesBuilder.add(node);
                    nodesChanged = true;
                    // 更新集群的最小和最大节点版本
                    minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                    maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                } catch (IllegalArgumentException | IllegalStateException e) {
                    // 如果加入失败，记录失败结果
                    results.failure(joinTask, e);
                    continue;
                }
                // 记录加入成功
                results.success(joinTask);
            }
        }
        // 如果节点有变化，则使用新的节点信息构建新的ClusterState并重新路由
        if (nodesChanged) {
            newState.nodes(nodesBuilder);
            return results.build(allocationService.reroute(newState.build(), "node_join"));
        } else {
            // 如果没有节点变化，仍然需要返回一个新的ClusterState实例以确保发布
            // 这对于加入节点完成其加入并设置我们为主节点很重要
            return results.build(newState.build());
        }
    }

    protected ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<Task> joiningNodes) {
        // 断言当前集群没有主节点
        assert currentState.nodes().getMasterNodeId() == null : currentState;
        // 获取当前的节点列表
        DiscoveryNodes currentNodes = currentState.nodes();
        // 创建节点列表构建器
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        // 设置当前节点为master节点
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

        // 遍历加入的节点
        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // 如果是成为主节点或完成选举任务，则无操作
            } else {
                // 获取加入的节点
                final DiscoveryNode joiningNode = joinTask.node();
                // 检查是否存在具有相同ID的现有节点，并且是否与加入节点不同
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    // 如果存在冲突，记录日志并从节点列表中移除现有节点
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                // 检查是否存在具有相同地址的现有节点，并且是否与加入节点不同
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    // 如果存在冲突，记录日志并从节点列表中移除现有节点
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress,
                        joiningNode);
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                }
            }
        }

        // 移除任何剩余的死亡节点，这些节点可能是之前主节点下线时遗留的
        // 或者是我们上面移除的
        ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(ClusterBlocks.builder()
                .blocks(currentState.blocks())
                .removeGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ID))
            .minimumMasterNodesOnPublishingMaster(minimumMasterNodesOnLocalNode)
            .build();
        // 记录日志
        logger.trace("becomeMasterAndTrimConflictingNodes: {}", tmpState.nodes());
        // 清理缓存
        allocationService.cleanCaches();
        // 从集群状态中移除与死亡节点的关联
        tmpState = PersistentTasksCustomMetaData.disassociateDeadNodes(tmpState);
        // 从集群状态中移除死亡节点
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

    @Override
    public boolean runOnlyOnMaster() {
        /**
         * 确定是否仅在主节点上运行此任务。
         * @return 如果此任务应该仅在主节点上运行，则返回true，否则返回false。
         */
        // 这个方法的实现表明，任务不是仅限于主节点执行，允许在非主节点上运行。
        return false;
    }

    public static Task newBecomeMasterTask() {
        /**
         * 创建一个新的成为主节点的任务。
         * @return 返回一个新的任务实例，该实例表示节点尝试成为主节点的操作。
         */
        return new Task(null, Task.BECOME_MASTER_TASK_REASON);
    }

    public static Task newFinishElectionTask() {
        /**
         * 创建一个新的结束选举的任务。
         * 这个任务用于信号选举过程已经停止，我们应该处理待定的节点加入请求。
         * 它可以与{@link JoinTaskExecutor#newBecomeMasterTask()}结合使用。
         * @return 返回一个新的任务实例，该实例表示选举过程已完成，需要处理后续的节点加入。
         */
        return new Task(null, Task.FINISH_ELECTION_TASK_REASON);
    }

    /**
     * Ensures that all indices are compatible with the given node version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of elasticsearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     * @see Version#minimumIndexCompatibilityVersion()
     * @throws IllegalStateException if any index is incompatible with the given version
     */
    public static void ensureIndexCompatibility(final Version nodeVersion, MetaData metaData) {
        Version supportedIndexVersion = nodeVersion.minimumIndexCompatibilityVersion();
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetaData idxMetaData : metaData) {
            if (idxMetaData.getCreationVersion().after(nodeVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " the node version is: " + nodeVersion);
            }
            if (idxMetaData.getCreationVersion().before(supportedIndexVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " minimum compatible index version is: " + supportedIndexVersion);
            }
        }
    }

    /** ensures that the joining node has a version that's compatible with all current nodes*/
    public static void ensureNodesCompatibility(final Version joiningNodeVersion, DiscoveryNodes currentNodes) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNodeVersion, minNodeVersion, maxNodeVersion);
    }

    /** ensures that the joining node has a version that's compatible with a given version range */
    public static void ensureNodesCompatibility(Version joiningNodeVersion, Version minClusterNodeVersion, Version maxClusterNodeVersion) {
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "The cluster contains nodes with version [" + maxClusterNodeVersion + "], which is incompatible.");
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported." +
                "The cluster contains nodes with version [" + minClusterNodeVersion + "], which is incompatible.");
        }
    }

    /**
     * ensures that the joining node's major version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the master is already fully operating under the new major version, it doesn't go back to mixed
     * version mode
     **/
    public static void ensureMajorVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        final byte clusterMajor = minClusterNodeVersion.major;
        if (joiningNodeVersion.major < clusterMajor) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "All nodes in the cluster are of a higher major [" + clusterMajor + "].");
        }
    }

    public static Collection<BiConsumer<DiscoveryNode,ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            ensureNodesCompatibility(node.getVersion(), state.getNodes());
            ensureIndexCompatibility(node.getVersion(), state.getMetaData());
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }
}
