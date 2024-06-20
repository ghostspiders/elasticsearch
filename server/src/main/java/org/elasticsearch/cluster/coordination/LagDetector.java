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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * 一个发布操作可以在所有节点应用并确认发布状态之前成功并完成；然而，我们需要每个节点最终要么应用发布的这个状态（或更晚的状态），要么从集群中移除。
 * 这个组件通过在超时后从集群中移除任何落后节点来实现这一点。
 */
public class LagDetector {

    private static final Logger logger = LogManager.getLogger(LagDetector.class);

    // 定义集群状态更新超时设置，这是每个节点在领导者应用更新后，应用更新之前的时间限制，如果超时则可能从集群中移除该节点
    public static final Setting<TimeValue> CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING =
        Setting.timeSetting(
            "cluster.follower_lag.timeout", // 设置的键名
            TimeValue.timeValueMillis(90000), // 默认值，90秒
            TimeValue.timeValueMillis(1), // 最小值，至少1毫秒
            Setting.Property.NodeScope // 作用域为节点级别
        );

    // 集群状态应用超时时间
    private final TimeValue clusterStateApplicationTimeout;
    // 当检测到节点落后时调用的消费者函数
    private final Consumer<DiscoveryNode> onLagDetected;
    // 提供本地节点信息的供应者函数
    private final Supplier<DiscoveryNode> localNodeSupplier;
    // 线程池，用于异步操作
    private final ThreadPool threadPool;
    // 存储每个节点及其对应的已应用状态追踪器的映射，使用线程安全的并发映射
    private final Map<DiscoveryNode, NodeAppliedStateTracker> appliedStateTrackersByNode = newConcurrentMap();

    public LagDetector(final Settings settings, final ThreadPool threadPool, final Consumer<DiscoveryNode> onLagDetected,
                       final Supplier<DiscoveryNode> localNodeSupplier) {
        this.threadPool = threadPool;
        this.clusterStateApplicationTimeout = CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.get(settings);
        this.onLagDetected = onLagDetected;
        this.localNodeSupplier = localNodeSupplier;
    }

    /**
     * 设置要追踪的节点集合。
     * @param discoveryNodes 要设置的节点集合。
     */
    public void setTrackedNodes(final Iterable<DiscoveryNode> discoveryNodes) {
        final Set<DiscoveryNode> discoveryNodeSet = new HashSet<>();
        // 将Iterable转换为Set。
        discoveryNodes.forEach(discoveryNodeSet::add);
        // 从集合中移除本地节点。
        discoveryNodeSet.remove(localNodeSupplier.get());
        // 保留在新集合中的节点对应的追踪器，移除其他节点的追踪器。
        appliedStateTrackersByNode.keySet().retainAll(discoveryNodeSet);
        // 为新集合中的每个节点创建追踪器，如果尚未创建。
        discoveryNodeSet.forEach(node -> appliedStateTrackersByNode.putIfAbsent(node, new NodeAppliedStateTracker(node)));
    }

    /**
     * 清除所有追踪的节点。
     */
    public void clearTrackedNodes() {
        // 清除所有节点状态追踪器。
        appliedStateTrackersByNode.clear();
    }

    /**
     * 设置指定节点已应用的版本。
     * @param discoveryNode 指定的节点。
     * @param appliedVersion 已应用的版本号。
     */
    public void setAppliedVersion(final DiscoveryNode discoveryNode, final long appliedVersion) {
        final NodeAppliedStateTracker nodeAppliedStateTracker = appliedStateTrackersByNode.get(discoveryNode);
        if (nodeAppliedStateTracker == null) {
            // 如果收到已移除节点的确认信息，或者当前不再是主节点，则无需追踪该节点的版本。
            logger.trace("node {} applied version {} but this node's version is not being tracked", discoveryNode, appliedVersion);
        } else {
            // 增加节点已应用的版本。
            nodeAppliedStateTracker.increaseAppliedVersion(appliedVersion);
        }
    }

    /**
     * 开始检测落后的节点。
     * @param version 要检测的版本号。
     */
    public void startLagDetector(final long version) {
        // 筛选出应用版本小于指定版本的节点状态追踪器。
        final List<NodeAppliedStateTracker> laggingTrackers
            = appliedStateTrackersByNode.values().stream().filter(t -> t.appliedVersionLessThan(version)).collect(Collectors.toList());

        if (laggingTrackers.isEmpty()) {
            // 如果没有落后的追踪器，则无需进行延迟检测。
            logger.trace("lag detection for version {} is unnecessary: {}", version, appliedStateTrackersByNode.values());
        } else {
            // 如果存在落后的追踪器，记录日志并启动延迟检测。
            logger.debug("starting lag detector for version {}: {}", version, laggingTrackers);

            // 根据设置的超时时间，在线程池中调度延迟检测任务。
            threadPool.scheduleUnlessShuttingDown(clusterStateApplicationTimeout, Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    // 对落后的追踪器进行检查。
                    laggingTrackers.forEach(t -> t.checkForLag(version));
                }

                @Override
                public String toString() {
                    // 提供任务的字符串表示，用于日志记录。
                    return "lag detector for version " + version + " on " + laggingTrackers;
                }
            });
        }
    }

    @Override
    public String toString() {
        return "LagDetector{" +
            "clusterStateApplicationTimeout=" + clusterStateApplicationTimeout +
            ", appliedStateTrackersByNode=" + appliedStateTrackersByNode.values() +
            '}';
    }

    // for assertions
    Set<DiscoveryNode> getTrackedNodes() {
        return Collections.unmodifiableSet(appliedStateTrackersByNode.keySet());
    }

    private class NodeAppliedStateTracker {
        private final DiscoveryNode discoveryNode;
        private final AtomicLong appliedVersion = new AtomicLong();

        NodeAppliedStateTracker(final DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        void increaseAppliedVersion(long appliedVersion) {
            long maxAppliedVersion = this.appliedVersion.updateAndGet(v -> Math.max(v, appliedVersion));
            logger.trace("{} applied version {}, max now {}", this, appliedVersion, maxAppliedVersion);
        }

        boolean appliedVersionLessThan(final long version) {
            return appliedVersion.get() < version;
        }

        @Override
        public String toString() {
            return "NodeAppliedStateTracker{" +
                "discoveryNode=" + discoveryNode +
                ", appliedVersion=" + appliedVersion +
                '}';
        }

        void checkForLag(final long version) {
            if (appliedStateTrackersByNode.get(discoveryNode) != this) {
                logger.trace("{} no longer active when checking version {}", this, version);
                return;
            }

            long appliedVersion = this.appliedVersion.get();
            if (version <= appliedVersion) {
                logger.trace("{} satisfied when checking version {}, node applied version {}", this, version, appliedVersion);
                return;
            }

            logger.debug("{}, detected lag at version {}, node has only applied version {}", this, version, appliedVersion);
            onLagDetected.accept(discoveryNode);
        }
    }
}
