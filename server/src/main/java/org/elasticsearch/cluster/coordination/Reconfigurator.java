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
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 计算集群中投票节点的最佳配置
 */
public class Reconfigurator {

    private static final Logger logger = LogManager.getLogger(Reconfigurator.class);

    /**
     投票配置的重要性：集群需要至少一半的主节点投票同意才能更新状态。如果自动调整投票配置，可以提高集群的容错性。

     调整投票配置的考虑：增加投票配置的大小是有益的，因为它可以提高集群在面对节点故障时的容错能力。然而，减少投票配置的大小可能不那么明智，因为它会降低容错性。例如，如果自动减少到只有一个节点，那么这个节点的故障将导致整个集群不可用。

     自动调整的策略：有两种策略可供选择：

     自动缩减：只要投票配置包含超过三个节点，就自动缩减投票配置。这种默认选项确保了只要集群中至少有三个主节点，并且其中不超过一个节点不可用，集群就能继续运行。
     手动控制：不自动缩减投票配置，而是要求用户使用退休API手动控制投票配置。这种方法适用于需要不同保证的用户。
     容错性的权衡：在五节点集群中，如果失去两个节点，通过将投票配置减少到剩下的三个节点，我们可以容忍进一步失去一个节点而不会导致集群失败。但是，如果不减少投票配置的大小，也可能影响集群的容错性。
     */
    /**
     * 定义了一个设置项，用于控制是否自动缩减集群的投票配置。
     * 默认值为true，表示开启自动缩减功能。这个设置项可以在节点级别动态更新。
     */
    public static final Setting<Boolean> CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION =
        Setting.boolSetting("cluster.auto_shrink_voting_configuration", true, Property.NodeScope, Property.Dynamic);

    /**
     * 用于存储自动缩减投票配置的状态，标记为volatile以确保多线程环境下的可见性。
     */
    private volatile boolean autoShrinkVotingConfiguration;

    /**
     * Reconfigurator类的构造函数。
     * @param settings 包含集群设置的Settings对象。
     * @param clusterSettings 用于管理集群设置的ClusterSettings对象。
     */
    public Reconfigurator(Settings settings, ClusterSettings clusterSettings) {
        // 从settings中获取自动缩减投票配置的状态，并存储。
        autoShrinkVotingConfiguration = CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(settings);
        // 注册设置更新的消费者，当CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION设置更新时，调用setAutoShrinkVotingConfiguration方法。
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION, this::setAutoShrinkVotingConfiguration);
    }

    /**
     * 更新自动缩减投票配置的状态。
     * @param autoShrinkVotingConfiguration 新的自动缩减状态。
     */
    public void setAutoShrinkVotingConfiguration(boolean autoShrinkVotingConfiguration) {
        this.autoShrinkVotingConfiguration = autoShrinkVotingConfiguration;
    }

    /**
     * 将输入的整数大小向下取到最近的奇数。
     * 如果输入是偶数，则减去1；如果是奇数，则保持不变。
     * @param size 输入的整数大小。
     * @return 向下取到最近的奇数值。
     */
    private static int roundDownToOdd(int size) {
        return size - (size % 2 == 0 ? 1 : 0);
    }

    @Override
    public String toString() {
        return "Reconfigurator{" +
            "autoShrinkVotingConfiguration=" + autoShrinkVotingConfiguration +
            '}';
    }

    /**
     * 计算集群的最优配置。
     *
     * @param liveNodes              集群中活跃的节点。最优配置尽可能优先选择活跃节点。
     * @param retiredNodeIds         即将离开集群的节点，如果可能的话，这些节点不应出现在配置中。如果已经退休的节点不在当前配置中，
     *                              那么它们也不会出现在结果配置中；这在两节点集群中非常有用，可以重新启动其中一个节点而不会影响可用性。
     * @param currentMaster          当前的主节点。除非已退休，我们更倾向于在配置中保留当前主节点。
     * @param currentConfig          当前的配置。尽可能保持当前配置不变。
     * @return 一个最优配置，或者如果最优配置没有活跃的法定人数，则保持当前配置不变。
     */
    public VotingConfiguration reconfigure(Set<DiscoveryNode> liveNodes, Set<String> retiredNodeIds, DiscoveryNode currentMaster,
                                           VotingConfiguration currentConfig) {
        // 断言检查，确保活跃节点不是Zen1节点，并且当前主节点包含在活跃节点中。
        assert liveNodes.stream().noneMatch(Coordinator::isZen1Node) : liveNodes;
        assert liveNodes.contains(currentMaster) : "liveNodes = " + liveNodes + " master = " + currentMaster;
        // 日志记录当前配置的重新配置过程。
        logger.trace("{} reconfiguring {} based on liveNodes={}, retiredNodeIds={}, currentMaster={}",
            this, currentConfig, liveNodes, retiredNodeIds, currentMaster);

        /*
         * 每个节点有三种属性：活跃/非活跃、退休/非退休和在配置中/不在配置中。
         * 首先，我们根据这些属性将节点划分为不同的集合：
         *
         * - 非退休主节点
         * - 非退休不在配置中且非活跃节点ID
         * - 非退休在配置中且活跃节点ID
         * - 非退休不在配置中且活跃节点ID
         *
         * 其他5种可能性不相关：
         * - 已退休，在配置中，活跃           -- 应从配置中移除退休节点
         * - 已退休，在配置中，非活跃         -- 同上
         * - 已退休，不在配置中，活跃         -- 不能将退休节点重新添加到配置中
         * - 已退休，不在配置中，非活跃       -- 同上
         * - 非退休，非活跃，不在配置中       -- 没有证据表明这个节点存在
         */

        // 计算活跃节点的ID集合。
        final Set<String> liveNodeIds = liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode).map(DiscoveryNode::getId).collect(Collectors.toSet());
        // 从当前配置中获取活跃节点的ID集合。
        final Set<String> liveInConfigIds = new TreeSet<>(currentConfig.getNodeIds());
        liveInConfigIds.retainAll(liveNodeIds);

        // 计算在配置中但不活跃的节点ID集合。
        final Set<String> inConfigNotLiveIds = Sets.sortedDifference(currentConfig.getNodeIds(), liveInConfigIds);
        // 计算非退休、在配置中但不活跃的节点ID集合。
        final Set<String> nonRetiredInConfigNotLiveIds = new TreeSet<>(inConfigNotLiveIds);
        nonRetiredInConfigNotLiveIds.removeAll(retiredNodeIds);

        // 计算非退休、在配置中且活跃的节点ID集合。
        final Set<String> nonRetiredInConfigLiveIds = new TreeSet<>(liveInConfigIds);
        nonRetiredInConfigLiveIds.removeAll(retiredNodeIds);

        // 根据当前主节点ID，进一步划分配置中活跃节点为两类：主节点和其他活跃节点。
        final Set<String> nonRetiredInConfigLiveMasterIds;
        final Set<String> nonRetiredInConfigLiveNotMasterIds;
        if (nonRetiredInConfigLiveIds.contains(currentMaster.getId())) {
            nonRetiredInConfigLiveNotMasterIds = new TreeSet<>(nonRetiredInConfigLiveIds);
            nonRetiredInConfigLiveNotMasterIds.remove(currentMaster.getId());
            nonRetiredInConfigLiveMasterIds = Collections.singleton(currentMaster.getId());
        } else {
            nonRetiredInConfigLiveNotMasterIds = nonRetiredInConfigLiveIds;
            nonRetiredInConfigLiveMasterIds = Collections.emptySet();
        }

        // 计算非退休、不在配置中且活跃的节点ID集合。
        final Set<String> nonRetiredLiveNotInConfigIds = Sets.sortedDifference(liveNodeIds, currentConfig.getNodeIds());
        nonRetiredLiveNotInConfigIds.removeAll(retiredNodeIds);

        /*
         * 现在我们计算配置中应该有多少节点：
         */
        final int targetSize;

        // 计算非退休活跃节点的总数。
        final int nonRetiredLiveNodeCount = nonRetiredInConfigLiveIds.size() + nonRetiredLiveNotInConfigIds.size();
        // 计算非退休配置中节点的总数。
        final int nonRetiredConfigSize = nonRetiredInConfigLiveIds.size() + nonRetiredInConfigNotLiveIds.size();
        if (autoShrinkVotingConfiguration) {
            if (nonRetiredLiveNodeCount >= 3) {
                // 如果非退休活跃节点数大于等于3，则目标大小为非退休活跃节点数向下取到最近的奇数。
                targetSize = roundDownToOdd(nonRetiredLiveNodeCount);
            } else {
                // 如果只有一到两个可用节点，可能不会自动缩减到3个节点以下，但如果配置（不包括退休节点）已经小于3，则可以。
                targetSize = nonRetiredConfigSize < 3 ? 1 : 3;
            }
        } else {
            // 如果不自动缩减，目标大小为非退休活跃节点数向下取到最近的奇数和非退休配置大小的较大者。
            targetSize = Math.max(roundDownToOdd(nonRetiredLiveNodeCount), nonRetiredConfigSize);
        }

        /*
         * 形成新的配置，按照以下优先顺序选择节点：
         */
        final VotingConfiguration newConfig = new VotingConfiguration(
            // 首先选择活跃的主节点，然后是其他活跃节点，优先考虑当前配置，如果需要更多节点，则使用非活跃节点。
            Stream.of(nonRetiredInConfigLiveMasterIds, nonRetiredInConfigLiveNotMasterIds, nonRetiredLiveNotInConfigIds,
                nonRetiredInConfigNotLiveIds).flatMap(Collection::stream).limit(targetSize).collect(Collectors.toSet())));

        // 如果新配置在活跃节点中有法定人数，则返回新配置；否则，如果新提出的配置中没有足够的活跃节点形成法定人数，最好什么都不做。
        return newConfig.hasQuorum(liveNodeIds) ? newConfig : currentConfig;
    }
}
