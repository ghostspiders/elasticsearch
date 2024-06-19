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

import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;

public class NoMasterBlockService {
    /**
     * 无主节点阻塞的ID常量。
     */
    public static final int NO_MASTER_BLOCK_ID = 2;

    /**
     * 无主节点时禁止写入的集群阻塞设置。
     */
    public static final ClusterBlock NO_MASTER_BLOCK_WRITES = new ClusterBlock(
        NO_MASTER_BLOCK_ID, "no master", true, false, false,
        RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );

    /**
     * 无主节点时禁止所有操作的集群阻塞设置。
     */
    public static final ClusterBlock NO_MASTER_BLOCK_ALL = new ClusterBlock(
        NO_MASTER_BLOCK_ID, "no master", true, true, false,
        RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL
    );

    /**
     * 已弃用的无主节点阻塞设置。
     */
    public static final Setting<ClusterBlock> LEGACY_NO_MASTER_BLOCK_SETTING =
        new Setting<>(
            "discovery.zen.no_master_block", "write", NoMasterBlockService::parseNoMasterBlock,
            Property.Dynamic, Property.NodeScope, Property.Deprecated
        );

    /**
     * 当前集群无主节点阻塞的设置。
     */
    public static final Setting<ClusterBlock> NO_MASTER_BLOCK_SETTING =
        new Setting<>(
            "cluster.no_master_block", "write", NoMasterBlockService::parseNoMasterBlock,
            Property.Dynamic, Property.NodeScope
        );

    /**
     * 当前的无主节点阻塞状态。
     */
    private volatile ClusterBlock noMasterBlock;

    /**
     * 构造函数，初始化无主节点阻塞服务。
     * @param settings 节点设置。
     * @param clusterSettings 集群设置。
     */
    public NoMasterBlockService(Settings settings, ClusterSettings clusterSettings) {
        // 初始化当前无主节点阻塞状态。
        this.noMasterBlock = NO_MASTER_BLOCK_SETTING.get(settings);
        // 添加设置更新的消费者，以便在集群设置更新时更改无主节点阻塞状态。
        clusterSettings.addSettingsUpdateConsumer(NO_MASTER_BLOCK_SETTING, this::setNoMasterBlock);

        // 处理已弃用的设置以发出弃用警告。
        LEGACY_NO_MASTER_BLOCK_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(LEGACY_NO_MASTER_BLOCK_SETTING, b -> {});
    }

    /**
     * 解析无主节点阻塞设置的字符串值。
     * @param value 字符串值。
     * @return 解析后的ClusterBlock枚举。
     */
    private static ClusterBlock parseNoMasterBlock(String value) {
        switch (value) {
            case "all":
                return NO_MASTER_BLOCK_ALL;
            case "write":
                return NO_MASTER_BLOCK_WRITES;
            default:
                throw new IllegalArgumentException(
                    "invalid no-master block [" + value + "], must be one of [all, write]");
        }
    }

    /**
     * 获取当前的无主节点阻塞状态。
     * @return 当前的无主节点阻塞状态。
     */
    public ClusterBlock getNoMasterBlock() {
        return noMasterBlock;
    }

    /**
     * 设置新的无主节点阻塞状态。
     * @param noMasterBlock 新的无主节点阻塞状态。
     */
    private void setNoMasterBlock(ClusterBlock noMasterBlock) {
        this.noMasterBlock = noMasterBlock;
    }
}
