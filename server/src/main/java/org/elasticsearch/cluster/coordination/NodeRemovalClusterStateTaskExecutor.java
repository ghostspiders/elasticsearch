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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;

import java.util.List;

public class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task>,
    ClusterStateTaskListener {

    private final AllocationService allocationService;
    private final Logger logger;

    public static class Task {

        private final DiscoveryNode node;
        private final String reason;

        public Task(final DiscoveryNode node, final String reason) {
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
            return node + " " + reason;
        }
    }

    public NodeRemovalClusterStateTaskExecutor(
            final AllocationService allocationService,
            final Logger logger) {
        this.allocationService = allocationService;
        this.logger = logger;
    }

    /**
     * 执行集群任务。
     * 此方法遍历任务列表，移除那些在当前集群状态中不存在的节点，然后根据结果更新集群状态。
     *
     * @param currentState 当前集群状态。
     * @param tasks 要执行的任务列表。
     * @return 包含执行结果的ClusterTasksResult对象。
     * @throws Exception 如果执行过程中发生异常。
     */
    @Override
    public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
        // 创建基于当前集群状态的节点构建器。
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
        boolean removed = false; // 标记是否移除了任何节点。

        // 遍历任务列表。
        for (final Task task : tasks) {
            // 如果当前集群状态中存在该节点，则从构建器中移除该节点。
            if (currentState.nodes().nodeExists(task.node())) {
                remainingNodesBuilder.remove(task.node());
                removed = true; // 设置标记为真。
            } else {
                // 如果节点不存在，则记录调试日志并忽略该任务。
                logger.debug("node [{}] does not exist in cluster state, ignoring", task);
            }
        }

        // 如果没有移除任何节点，则保持当前集群状态并返回所有任务的成功结果。
        if (!removed) {
            return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
        }

        // 如果移除了节点，使用剩余节点的构建器创建新的集群状态。
        final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);

        // 根据当前状态、任务和剩余节点的集群状态获取任务的集群任务结果。
        return getTaskClusterTasksResult(currentState, tasks, remainingNodesClusterState);
    }

    protected ClusterTasksResult<Task> getTaskClusterTasksResult(ClusterState currentState, List<Task> tasks,
                                                                 ClusterState remainingNodesClusterState) {
        ClusterState ptasksDisassociatedState = PersistentTasksCustomMetaData.disassociateDeadNodes(remainingNodesClusterState);
        final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
        return resultBuilder.build(allocationService.disassociateDeadNodes(ptasksDisassociatedState, true, describeTasks(tasks)));
    }

    // visible for testing
    // hook is used in testing to ensure that correct cluster state is used to test whether a
    // rejoin or reroute is needed
    protected ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
        return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
    }

    @Override
    public void onFailure(final String source, final Exception e) {
        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
    }

    @Override
    public void onNoLongerMaster(String source) {
        logger.debug("no longer master while processing node removal [{}]", source);
    }

}
