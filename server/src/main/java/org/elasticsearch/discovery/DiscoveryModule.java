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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

/**
 * A module for loading classes for node discovery.
 */
public class DiscoveryModule {
    // 创建一个Logger实例，用于记录日志信息，与DiscoveryModule类关联
    private static final Logger logger = LogManager.getLogger(DiscoveryModule.class);

    // 定义了使用旧版Zen发现机制的字符串常量
    public static final String ZEN_DISCOVERY_TYPE = "legacy-zen";
    // 定义了使用新版Zen2发现机制的字符串常量
    public static final String ZEN2_DISCOVERY_TYPE = "zen";

    // 定义了单节点发现机制的字符串常量，用于单一节点运行Elasticsearch时
    public static final String SINGLE_NODE_DISCOVERY_TYPE = "single-node";

    // 发现类型设置，指定了默认值为ZEN2_DISCOVERY_TYPE，并且设置了作用域为节点级别
    public static final Setting<String> DISCOVERY_TYPE_SETTING =
        new Setting<>("discovery.type", ZEN2_DISCOVERY_TYPE, Function.identity(), Property.NodeScope);
    // 已弃用的Zen发现种子节点提供者设置，用于配置旧版Zen发现的种子节点
    public static final Setting<List<String>> LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING =
        Setting.listSetting("discovery.zen.hosts_provider", Collections.emptyList(), Function.identity(),
            Property.NodeScope, Property.Deprecated);
    // 种子节点提供者设置，用于配置新版Zen2发现的种子节点
    public static final Setting<List<String>> DISCOVERY_SEED_PROVIDERS_SETTING =
        Setting.listSetting("discovery.seed_providers", Collections.emptyList(), Function.identity(),
            Property.NodeScope);

    // 当前DiscoveryModule的发现服务实例
    private final Discovery discovery;

    public DiscoveryModule(Settings settings, ThreadPool threadPool, TransportService transportService,
                           NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService, MasterService masterService,
                           ClusterApplier clusterApplier, ClusterSettings clusterSettings, List<DiscoveryPlugin> plugins,
                           AllocationService allocationService, Path configFile, GatewayMetaState gatewayMetaState) {
        // 初始化节点加入验证器的集合
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators = new ArrayList<>();
        // 初始化种子主机提供者的映射
        final Map<String, Supplier<SeedHostsProvider>> hostProviders = new HashMap<>();
        // 添加基于设置的种子主机提供者
        hostProviders.put("settings", () -> new SettingsBasedSeedHostsProvider(settings, transportService));
        // 添加基于文件的种子主机提供者
        hostProviders.put("file", () -> new FileBasedSeedHostsProvider(configFile));
        // 遍历插件，注册种子主机提供者和节点加入验证器
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getSeedHostProviders(transportService, networkService).forEach((key, value) -> {
                if (hostProviders.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register seed provider [" + key + "] twice");
                }
            });
            BiConsumer<DiscoveryNode, ClusterState> joinValidator = plugin.getJoinValidator();
            if (joinValidator != null) {
                joinValidators.add(joinValidator);
            }
        }

        // 获取种子提供者的名称列表
        List<String> seedProviderNames = getSeedProviderNames(settings);
        // 为了向后兼容，确保设置提供者被包含在内
        if (!seedProviderNames.contains("settings")) {
            List<String> extendedSeedProviderNames = new ArrayList<>(seedProviderNames);
            extendedSeedProviderNames.add(0, "settings"); // 确保设置提供者是第一个
            seedProviderNames = extendedSeedProviderNames;
        }

        // 检查是否有未识别的种子提供者名称
        final Set<String> missingProviderNames = new HashSet<>(seedProviderNames);
        missingProviderNames.removeAll(hostProviders.keySet());
        if (!missingProviderNames.isEmpty()) {
            throw new IllegalArgumentException("Unknown seed providers " + missingProviderNames);
        }

        // 创建种子主机提供者列表
        List<SeedHostsProvider> filteredSeedProviders = seedProviderNames.stream()
            .map(hostProviders::get).map(Supplier::get).collect(Collectors.toList());

        // 获取发现类型
        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);

        // 创建种子主机提供者实例
        final SeedHostsProvider seedHostsProvider = hostsResolver -> {
            List<TransportAddress> addresses = new ArrayList<>();
            for (SeedHostsProvider provider : filteredSeedProviders) {
                addresses.addAll(provider.getSeedAddresses(hostsResolver));
            }
            return Collections.unmodifiableList(addresses);
        };

        // 根据发现类型创建相应的发现服务实例
        if (ZEN2_DISCOVERY_TYPE.equals(discoveryType) || SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType)) {
            discovery = new Coordinator(NODE_NAME_SETTING.get(settings),
                settings, clusterSettings,
                transportService, namedWriteableRegistry, allocationService, masterService,
                () -> gatewayMetaState.getPersistedState(settings, (ClusterApplierService) clusterApplier), seedHostsProvider,
                clusterApplier, joinValidators, new Random(Randomness.get().nextLong()));
        } else if (ZEN_DISCOVERY_TYPE.equals(discoveryType)) {
            discovery = new ZenDiscovery(settings, threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier,
                clusterSettings, seedHostsProvider, allocationService, joinValidators, gatewayMetaState);
        } else {
            throw new IllegalArgumentException("Unknown discovery type [" + discoveryType + "]");
        }

        // 记录使用的发现类型和种子主机提供者信息
        logger.info("using discovery type [{}] and seed hosts providers {}", discoveryType, seedProviderNames);
    }
    private List<String> getSeedProviderNames(Settings settings) {
        if (LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.exists(settings)) {
            if (DISCOVERY_SEED_PROVIDERS_SETTING.exists(settings)) {
                throw new IllegalArgumentException("it is forbidden to set both [" + DISCOVERY_SEED_PROVIDERS_SETTING.getKey() + "] and ["
                    + LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.getKey() + "]");
            }
            return LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.get(settings);
        }
        return DISCOVERY_SEED_PROVIDERS_SETTING.get(settings);
    }

    public Discovery getDiscovery() {
        return discovery;
    }
}
