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

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.common.component.LifecycleComponent;

/**
 * A pluggable module allowing to implement discovery of other nodes, publishing of the cluster
 * state to all nodes, electing a master of the cluster that raises cluster state change
 * events.
 */
public interface Discovery extends LifecycleComponent, ClusterStatePublisher {

    /**
     * 获取关于节点发现的统计信息。
     * <p>
     * 这个方法返回一个包含了节点发现各种统计数据的对象，这些数据可以用于监控和调试。
     * <p>
     * @return 包含节点发现统计信息的对象
     */
    DiscoveryStats stats();

    /**
     * 触发初始的加入周期。
     * <p>
     * 这个方法启动节点加入集群的初始过程，通常用于节点启动时寻找并加入现有的集群。
     * <p>
     * 在这个过程中，节点会尝试与集群中的其他节点建立连接，并参与到主节点选举和集群状态同步中。
     * <p>
     * 这个方法通常在节点启动时被调用一次，但在某些情况下，如集群分裂恢复后，可能会再次触发。
     */
    void startInitialJoin();

}
