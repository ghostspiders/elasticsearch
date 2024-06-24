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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

public interface ClusterStatePublisher {
    /**
     * 这个方法由主节点调用，用于将所有变更发布到集群中（只能由主节点调用）。发布过程也应该将这个状态应用到主节点上！
     *
     * publishListener 允许等待发布过程完成，这可以是成功完成、超时或失败。
     * 如果变更没有被提交并且应该被拒绝，该方法保证会通过 publishListener 传递一个 FailedToCommitClusterStateException。
     * 任何其他异常表示发生了某些问题，但变更已经被提交。
     *
     * AckListener 允许跟踪从节点收到的确认，并验证它们是否更新了自己的集群状态。
     */
    void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener);

    /**
     * AckListener 接口
     */
    interface AckListener {
        /**
         * 当集群协调层提交了集群状态时调用（即使这次发布失败，也保证这个变更会出现在未来的发布中）。
         * @param commitTime 提交集群状态所花费的时间
         */
        void onCommit(TimeValue commitTime);

        /**
         * 每当集群协调层从节点接收到确认，该节点已成功应用集群状态时调用。
         * 如果发生失败，应该提供一个异常作为参数。
         * @param node 确认的节点
         * @param e 可选的异常对象
         */
        void onNodeAck(DiscoveryNode node, @Nullable Exception e);
    }

}
