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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.discovery.zen.ZenPing.PingResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.cluster.coordination.Coordinator.isZen1Node;
import static org.elasticsearch.cluster.coordination.DiscoveryUpgradeService.createDiscoveryNodeWithImpossiblyHighId;

public abstract class PeerFinder {

    // 导入日志记录器
    private static final Logger logger = LogManager.getLogger(PeerFinder.class);

    // 请求节点信息的操作名称
    public static final String REQUEST_PEERS_ACTION_NAME = "internal:discovery/request_peers";

    // 定义设置项：发现所有节点之间尝试的时间间隔
    public static final Setting<TimeValue> DISCOVERY_FIND_PEERS_INTERVAL_SETTING =
        Setting.timeSetting("discovery.find_peers_interval",
            TimeValue.timeValueMillis(1000), // 默认值1秒
            TimeValue.timeValueMillis(1),     // 最小值1毫秒
            Setting.Property.NodeScope);      // 作用域为节点

    // 定义设置项：请求节点信息的超时时间
    public static final Setting<TimeValue> DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.request_peers_timeout",
            TimeValue.timeValueMillis(3000), // 默认值3秒
            TimeValue.timeValueMillis(1),     // 最小值1毫秒
            Setting.Property.NodeScope);      // 作用域为节点

    // 节点设置
    private final Settings settings;

    // 发现节点的时间间隔
    private final TimeValue findPeersInterval;
    // 请求节点信息的超时时间
    private final TimeValue requestPeersTimeout;

    // 同步锁
    private final Object mutex = new Object();
    // 传输服务，用于节点间的通信
    private final TransportService transportService;
    // 传输地址连接器
    private final TransportAddressConnector transportAddressConnector;
    // 配置的主机解析器
    private final ConfiguredHostsResolver configuredHostsResolver;

    // 当前任期
    private volatile long currentTerm;
    // 是否处于活跃状态
    private boolean active;
    // 上一次接受的节点列表
    private DiscoveryNodes lastAcceptedNodes;
    // 通过传输地址索引的节点映射
    private final Map<TransportAddress, Peer> peersByAddress = new LinkedHashMap<>();
    // 可选的领导者节点
    private Optional<DiscoveryNode> leader = Optional.empty();
    // 上次解析的地址列表
    private volatile List<TransportAddress> lastResolvedAddresses = emptyList();

    /**
     * PeerFinder类的构造函数。
     * 初始化节点发现服务，包括设置项、时间间隔、传输服务等。
     *
     * @param settings 包含节点配置的Settings对象。
     * @param transportService 用于节点间通信的传输服务。
     * @param transportAddressConnector 用于连接传输地址的服务。
     * @param configuredHostsResolver 用于解析配置的主机地址的服务。
     */
    public PeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                      ConfiguredHostsResolver configuredHostsResolver) {
        this.settings = settings; // 初始化设置对象
        findPeersInterval = DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(settings); // 获取节点发现的时间间隔设置
        requestPeersTimeout = DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING.get(settings); // 获取请求节点信息的超时设置
        this.transportService = transportService; // 初始化传输服务
        this.transportAddressConnector = transportAddressConnector; // 初始化传输地址连接器
        this.configuredHostsResolver = configuredHostsResolver; // 初始化配置的主机解析器

        // 注册请求处理器，用于处理节点发现请求
        transportService.registerRequestHandler(REQUEST_PEERS_ACTION_NAME, Names.GENERIC, false, false,
            PeersRequest::new, // 使用PeersRequest构造函数创建请求对象
            (request, channel, task) -> channel.sendResponse(handlePeersRequest(request)) // 处理请求并发送响应
        );

        // 注册请求处理器，用于处理单播ZenPing请求
        transportService.registerRequestHandler(UnicastZenPing.ACTION_NAME, Names.GENERIC, false, false,
            UnicastZenPing.UnicastPingRequest::new, // 使用UnicastPingRequest构造函数创建请求对象
            new Zen1UnicastPingRequestHandler() // 使用Zen1UnicastPingRequestHandler处理请求
        );
    }

    /**
     * 激活节点发现服务。
     * 此方法将启动节点发现过程，尝试连接到已知的节点。
     *
     * @param lastAcceptedNodes 上次接受的节点列表，包含活跃和非活跃的节点。
     */
    public void activate(final DiscoveryNodes lastAcceptedNodes) {
        logger.trace("activating with {}", lastAcceptedNodes); // 记录激活时使用的节点列表

        synchronized (mutex) { // 进入同步块以保证线程安全
            // 断言当前没有活跃的连接且没有已知的节点
            assert assertInactiveWithNoKnownPeers();
            active = true; // 设置节点发现服务为活跃状态
            this.lastAcceptedNodes = lastAcceptedNodes; // 更新上次接受的节点列表
            leader = Optional.empty(); // 重置领导者节点为无（可选为空）
            handleWakeUp(); // 处理唤醒逻辑，即使没有已知节点也不会断开任何连接
        }

        onFoundPeersUpdated(); // 触发检查是否已经有法定人数的更新
    }

    /**
     * 停用节点发现服务，并设置当前的领导者节点。
     * 当节点发现服务不再活跃时，将移除所有已知的节点连接，并设置一个新的领导者节点。
     *
     * @param leader 新的领导者节点。
     */
    public void deactivate(DiscoveryNode leader) {
        final boolean peersRemoved; // 标记是否移除了节点
        synchronized (mutex) { // 进入同步块以保证线程安全
            logger.trace("deactivating and setting leader to {}", leader); // 记录停用操作和新的领导者节点
            active = false; // 设置节点发现服务为非活跃状态
            peersRemoved = handleWakeUp(); // 处理唤醒逻辑，移除所有已知节点连接
            this.leader = Optional.of(leader); // 设置新的领导者节点
            assert assertInactiveWithNoKnownPeers(); // 断言当前没有活跃的连接且没有已知的节点
        }
        // 如果有节点被移除，则触发检查已知节点更新的事件
        if (peersRemoved) {
            onFoundPeersUpdated();
        }
    }

    /**
     * 供子类测试使用，检查当前线程是否持有指定的锁。
     * @return 如果当前线程持有mutex锁，则返回true。
     */
    protected final boolean holdsLock() {
        return Thread.holdsLock(mutex); // 检查当前线程是否持有mutex锁
    }

    /**
     * 断言当前PeerFinder实例不在活跃状态，并且没有已知的节点。
     * 这是一个辅助方法，用于在关键操作之前确保状态的正确性。
     *
     * @return 如果状态满足条件（非活跃且没有已知节点），返回true。
     */
    private boolean assertInactiveWithNoKnownPeers() {
        assert holdsLock() : "PeerFinder mutex not held"; // 断言当前线程持有mutex锁
        assert active == false; // 断言PeerFinder不在活跃状态
        assert peersByAddress.isEmpty() : peersByAddress.keySet(); // 断言没有已知的节点
        return true; // 如果所有断言都通过，则返回true
    }

    /**
     * 处理节点发现请求。
     * 当收到来自其他节点的节点发现请求时，此方法将被调用。
     *
     * @param peersRequest 节点发现请求对象。
     * @return 返回节点发现响应对象，包含当前领导者、已知节点列表和当前任期。
     */
    public PeersResponse handlePeersRequest(PeersRequest peersRequest) {
        synchronized (mutex) { // 进入同步块以保证线程安全
            // 断言请求来源节点不是本地节点
            assert peersRequest.getSourceNode().equals(getLocalNode()) == false;

            List<DiscoveryNode> knownPeers; // 已知节点列表
            if (active) { // 如果节点发现服务处于活跃状态
                // 断言当前不应该有领导者
                assert leader.isPresent() == false : leader;
                // 如果请求来源节点是主节点，开始探测其地址
                if (peersRequest.getSourceNode().isMasterNode()) {
                    startProbe(peersRequest.getSourceNode().getAddress());
                }
                // 对请求中包含的已知节点列表进行处理，开始探测它们的地址
                peersRequest.getKnownPeers().stream()
                    .map(DiscoveryNode::getAddress)
                    .forEach(this::startProbe);
                // 获取当前发现的节点列表
                knownPeers = getFoundPeersUnderLock();
            } else { // 如果节点发现服务不处于活跃状态
                // 断言当前应该有领导者或没有接受的节点列表
                assert leader.isPresent() || lastAcceptedNodes == null;
                // 已知节点列表为空
                knownPeers = emptyList();
            }
            // 创建并返回节点发现响应，包含当前领导者、已知节点列表和当前任期
            return new PeersResponse(leader, knownPeers, currentTerm);
        }
    }

    /**
     * 获取当前的领导者节点。
     * 此方法被公开以便在其他包中的o.e.c.c.Coordinator检查不变性。
     * @return 当前领导者节点的Optional包装，如果没有领导者则返回Optional.empty()。
     */
    public Optional<DiscoveryNode> getLeader() {
        synchronized (mutex) { // 进入同步块以保证线程安全
            return leader; // 返回当前领导者节点的Optional
        }
    }

    /**
     * 获取当前的任期。
     * 此方法被公开以便在其他包中的o.e.c.c.Coordinator检查不变性。
     * @return 当前的任期。
     */
    public long getCurrentTerm() {
        return currentTerm; // 返回当前任期
    }

    /**
     * 设置当前的任期。
     * @param currentTerm 新的当前任期。
     */
    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm; // 更新当前任期
    }

    /**
     * 获取本地节点信息。
     * 此方法通常用于获取通过传输服务注册的本地节点信息。
     * @return 本地节点的DiscoveryNode对象。
     */
    private DiscoveryNode getLocalNode() {
        final DiscoveryNode localNode = transportService.getLocalNode(); // 从传输服务获取本地节点
        assert localNode != null; // 断言本地节点不为空
        return localNode; // 返回本地节点
    }
    /**
     * 当收到一个认为自己是活跃领导者的节点的PeersResponse时调用。
     * 因此，此节点应尝试加入该领导者节点。
     * 注意，调用此方法时没有进行同步。到调用时我们可能已经被停用。
     *
     * @param masterNode 认为自身是活跃领导者的节点。
     * @param term 当前的任期。
     */
    protected abstract void onActiveMasterFound(DiscoveryNode masterNode, long term);

    /**
     * 当找到的节点集合发生变化时调用。
     * 注意，调用此方法时没有完全同步，我们只保证在调用此方法之前集合发生变化。
     * 如果有多个并发变化，那么会有多个此方法的并发调用，不保证它们的顺序。
     * 由于这个原因，我们不将更新后的节点集合作为参数传递给此方法，而是让实现者调用getFoundPeers()并适当同步以避免更新丢失。
     * 同样，到调用此方法时我们可能已经被停用。
     */
    protected abstract void onFoundPeersUpdated();

    /**
     * 获取最后解析的地址列表。
     * @return 返回最后解析的地址列表。
     */
    public List<TransportAddress> getLastResolvedAddresses() {
        return lastResolvedAddresses; // 返回最后解析的地址列表
    }

    public interface TransportAddressConnector {
        /**
         * Identify the node at the given address and, if it is a master node and not the local node then establish a full connection to it.
         */
        void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener);
    }

    public interface ConfiguredHostsResolver {
        /**
         * Attempt to resolve the configured unicast hosts list to a list of transport addresses.
         *
         * @param consumer Consumer for the resolved list. May not be called if an error occurs or if another resolution attempt is in
         *                 progress.
         */
        void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer);
    }

    /**
     * 获取当前发现的节点集合。
     * 此方法通过持有mutex锁来确保线程安全，并调用私有辅助方法来获取节点列表。
     * @return 一个包含当前发现的节点的Iterable对象。
     */
    public Iterable<DiscoveryNode> getFoundPeers() {
        synchronized (mutex) { // 进入同步块以保证线程安全
            return getFoundPeersUnderLock(); // 调用私有方法获取节点列表
        }
    }

    /**
     * 在持有mutex锁的情况下获取当前发现的节点列表。
     * 此方法是一个私有辅助方法，用于从peersByAddress映射中提取节点信息，并确保返回的节点不重复。
     * @return 一个包含当前发现的节点的列表。
     */
    private List<DiscoveryNode> getFoundPeersUnderLock() {
        assert holdsLock() : "PeerFinder mutex not held"; // 断言当前线程持有mutex锁
        return peersByAddress.values().stream() // 从映射值中获取所有Peer对象
            .map(Peer::getDiscoveryNode) // 将Peer对象转换为DiscoveryNode对象
            .filter(Objects::nonNull) // 过滤掉null值
            .distinct() // 去除重复的节点
            .collect(Collectors.toList()); // 收集结果到列表中
    }

    /**
     * 为给定的传输地址创建一个正在连接的Peer对象。
     * @param transportAddress 要连接的节点的传输地址。
     * @return 创建的Peer对象。
     */
    private Peer createConnectingPeer(TransportAddress transportAddress) {
        Peer peer = new Peer(transportAddress); // 使用传输地址创建Peer对象
        peer.establishConnection(); // 建立连接
        return peer; // 返回新创建的Peer对象
    }
    /**
     * 处理唤醒事件，例如在节点发现过程中节点断开连接。
     * @return 如果由于断开连接而移除了任何节点，则返回true。
     */
    private boolean handleWakeUp() {
        assert holdsLock() : "PeerFinder mutex not held"; // 断言当前线程持有mutex锁

        boolean peersRemoved = peersByAddress.values().removeIf(Peer::handleWakeUp); // 移除由于断开连接的节点

        if (!active) { // 如果PeerFinder不处于活跃状态
            logger.trace("not active"); // 记录状态
            return peersRemoved; // 返回是否有节点被移除
        }

        logger.trace("probing master nodes from cluster state: {}", lastAcceptedNodes); // 记录正在探测的集群状态中的主节点
        for (ObjectCursor<DiscoveryNode> discoveryNodeObjectCursor : lastAcceptedNodes.getMasterNodes().values()) {
            startProbe(discoveryNodeObjectCursor.value.getAddress()); // 开始探测集群状态中的主节点地址
        }

        configuredHostsResolver.resolveConfiguredHosts(providedAddresses -> { // 解析配置的主机地址
            synchronized (mutex) {
                lastResolvedAddresses = providedAddresses; // 更新最后解析的地址列表
                logger.trace("probing resolved transport addresses {}", providedAddresses); // 记录正在探测的解析后的传输地址
                providedAddresses.forEach(this::startProbe); // 开始探测解析后的地址
            }
        });

        transportService.getThreadPool().scheduleUnlessShuttingDown( // 计划执行唤醒处理任务
            findPeersInterval, // 根据设置的时间间隔
            Names.GENERIC, // 线程池的任务名称
            new AbstractRunnable() { // 创建一个新的Runnable任务
                @Override
                public boolean isForceExecution() {
                    return true; // 强制执行此任务
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : e; // 如果发生异常，断言失败
                    logger.debug("unexpected exception in wakeup", e); // 记录异常信息
                }

                @Override
                protected void doRun() {
                    synchronized (mutex) {
                        if (!handleWakeUp()) { // 再次处理唤醒事件
                            return;
                        }
                    }
                    onFoundPeersUpdated(); // 如果有节点更新，触发相关事件
                }

                @Override
                public String toString() {
                    return "PeerFinder handling wakeup"; // 任务的字符串表示
                }
            }
        );

        return peersRemoved; // 返回是否有节点被移除
    }
    /**
     * 开始对指定传输地址的节点进行探测。
     * 此方法用于初始化与远程节点的连接过程。
     *
     * @param transportAddress 要探测的节点的传输地址。
     */
    protected void startProbe(TransportAddress transportAddress) {
        assert holdsLock() : "PeerFinder mutex not held"; // 断言当前线程持有mutex锁

        if (!active) { // 如果PeerFinder不处于活跃状态
            logger.trace("startProbe({}) not running", transportAddress); // 记录不运行的状态
            return;
        }

        if (transportAddress.equals(getLocalNode().getAddress())) { // 如果传输地址与本地节点地址相同
            logger.trace("startProbe({}) not probing local node", transportAddress); // 记录不探测本地节点的状态
            return;
        }

        // 使用computeIfAbsent方法来处理传输地址
        // 如果peersByAddress中没有对应的传输地址，则使用createConnectingPeer方法创建一个新的Peer对象
        peersByAddress.computeIfAbsent(transportAddress, this::createConnectingPeer);
    }


    private class Peer {

        /**
         * 传输地址，表示此Peer节点的网络地址。
         */
        private final TransportAddress transportAddress;

        /**
         * 用于确保DiscoveryNode对象只被设置一次的包装器。
         * SetOnce类确保discoveryNode变量在初始化后不能被再次修改。
         */
        private SetOnce<DiscoveryNode> discoveryNode = new SetOnce<>();

        /**
         * 标记是否对等方的节点请求（PeersRequest）正在进行中。
         * 这是一个volatile变量，确保多线程环境下对这个状态的修改是可见的。
         */
        private volatile boolean peersRequestInFlight;
        Peer(TransportAddress transportAddress) {
            this.transportAddress = transportAddress;
        }

        @Nullable
        DiscoveryNode getDiscoveryNode() {
            return discoveryNode.get();
        }

        /**
         * 处理唤醒事件。
         * 此方法在某些条件触发时被调用，例如网络事件或定时任务。
         * @return 如果由于断开连接而移除了节点，则返回true。
         */
        boolean handleWakeUp() {
            assert holdsLock() : "PeerFinder mutex not held"; // 断言当前线程持有PeerFinder的mutex锁

            if (active == false) { // 如果PeerFinder不处于活跃状态
                return true; // 返回true，表示节点被移除
            }

            final DiscoveryNode discoveryNode = getDiscoveryNode(); // 获取DiscoveryNode对象
            // 可能为null，如果连接尚未建立

            if (discoveryNode != null) { // 如果连接已建立
                if (transportService.nodeConnected(discoveryNode)) { // 如果节点仍然连接
                    if (peersRequestInFlight == false) { // 如果没有正在进行的节点请求
                        requestPeers(); // 请求已知节点信息
                    }
                } else {
                    logger.trace("{} no longer connected", this); // 记录节点已不再连接
                    return true; // 返回true，表示节点被移除
                }
            }

            return false; // 没有节点被移除
        }

        /**
         * 建立与远程节点的连接。
         * 此方法尝试与远程节点建立连接，并在成功时更新状态。
         */
        void establishConnection() {
            assert holdsLock() : "PeerFinder mutex not held"; // 断言当前线程持有PeerFinder的mutex锁
            assert getDiscoveryNode() == null : "unexpectedly connected to " + getDiscoveryNode(); // 断言尚未建立连接
            assert active; // 断言PeerFinder处于活跃状态

            logger.trace("{} attempting connection", this); // 记录尝试连接

            transportAddressConnector.connectToRemoteMasterNode(transportAddress, // 使用传输地址连接器连接到远程主节点
                new ActionListener<DiscoveryNode>() {
                    @Override
                    public void onResponse(DiscoveryNode remoteNode) { // 连接成功的回调
                        assert remoteNode.isMasterNode() : remoteNode + " is not master-eligible"; // 断言远程节点是主节点
                        assert remoteNode.equals(getLocalNode()) == false : remoteNode + " is the local node"; // 断言远程节点不是本地节点
                        synchronized (mutex) {
                            if (active == false) {
                                return;
                            }

                            assert discoveryNode.get() == null : "discoveryNode unexpectedly already set"; // 断言discoveryNode尚未设置
                            discoveryNode.set(remoteNode); // 设置远程节点信息
                            requestPeers(); // 请求已知节点信息
                        }

                        assert holdsLock() == false : "PeerFinder mutex is held in error"; // 断言当前没有持有mutex锁
                        onFoundPeersUpdated(); // 触发找到节点更新的事件
                    }

                    @Override
                    public void onFailure(Exception e) { // 连接失败的回调
                        logger.debug(() -> new ParameterizedMessage("{} connection failed", Peer.this), e); // 记录连接失败
                        synchronized (mutex) {
                            peersByAddress.remove(transportAddress); // 从映射中移除传输地址
                        }
                    }
                }
            );
        }
        /**
         * 请求已知节点列表。
         * 此方法用于向远程节点请求其已知的节点列表。
         */
        private void requestPeers() {
            assert holdsLock() : "PeerFinder mutex not held";
            // 断言当前线程持有PeerFinder的mutex锁
            assert peersRequestInFlight == false : "PeersRequest already in flight";
            // 断言没有正在进行的节点请求
            assert active;
            // 断言PeerFinder处于活跃状态
            final DiscoveryNode discoveryNode = getDiscoveryNode();
            // 获取DiscoveryNode对象
            assert discoveryNode != null : "cannot request peers without first connecting";
            // 断言连接已建立
            if (discoveryNode.equals(getLocalNode())) {
                logger.trace("{} not requesting peers from local node", this);
                return;
            }

            logger.trace("{} requesting peers", this);
            peersRequestInFlight = true;

            final List<DiscoveryNode> knownNodes = getFoundPeersUnderLock();
            // 定义处理节点响应的处理器
            final TransportResponseHandler<PeersResponse> peersResponseHandler = new TransportResponseHandler<PeersResponse>() {
                // 覆盖read方法来从输入流中读取PeersResponse
                @Override
                public PeersResponse read(StreamInput in) throws IOException {
                    return new PeersResponse(in);
                }
                // 覆盖handleResponse方法来处理响应
                @Override
                public void handleResponse(PeersResponse response) {
                    logger.trace("{} received {}", Peer.this, response);
                    synchronized (mutex) {
                        if (active == false) {
                            return;
                        }

                        peersRequestInFlight = false;

                        response.getMasterNode().map(DiscoveryNode::getAddress).ifPresent(PeerFinder.this::startProbe);
                        response.getKnownPeers().stream().map(DiscoveryNode::getAddress).forEach(PeerFinder.this::startProbe);
                    }
                    // 如果响应中的主节点是当前节点，则触发活动主节点发现事件
                    if (response.getMasterNode().equals(Optional.of(discoveryNode))) {
                        // Must not hold lock here to avoid deadlock
                        assert holdsLock() == false : "PeerFinder mutex is held in error";
                        onActiveMasterFound(discoveryNode, response.getTerm());
                    }
                }
                // 覆盖handleException方法来处理请求过程中的异常
                @Override
                public void handleException(TransportException exp) {
                    peersRequestInFlight = false;
                    logger.debug(new ParameterizedMessage("{} peers request failed", Peer.this), exp);
                }
                // 覆盖executor方法来指定处理响应的线程池
                @Override
                public String executor() {
                    return Names.GENERIC;
                }
            };
            // 根据是否是Zen1节点构造请求
            final String actionName;
            final TransportRequest transportRequest;
            final TransportResponseHandler<?> transportResponseHandler;
            if (isZen1Node(discoveryNode)) {
                actionName = UnicastZenPing.ACTION_NAME;
                transportRequest = new UnicastZenPing.UnicastPingRequest(1, ZenDiscovery.PING_TIMEOUT_SETTING.get(settings),
                    new ZenPing.PingResponse(createDiscoveryNodeWithImpossiblyHighId(getLocalNode()), null,
                        ClusterName.CLUSTER_NAME_SETTING.get(settings), ClusterState.UNKNOWN_VERSION));
                transportResponseHandler = peersResponseHandler.wrap(ucResponse -> {
                    Optional<DiscoveryNode> optionalMasterNode = Arrays.stream(ucResponse.pingResponses)
                        .filter(pr -> discoveryNode.equals(pr.node()) && discoveryNode.equals(pr.master()))
                        .map(ZenPing.PingResponse::node)
                        .findFirst();
                    List<DiscoveryNode> discoveredNodes = new ArrayList<>();
                    if (optionalMasterNode.isPresent() == false) {
                        Arrays.stream(ucResponse.pingResponses).map(PingResponse::master).filter(Objects::nonNull)
                            .forEach(discoveredNodes::add);
                        Arrays.stream(ucResponse.pingResponses).map(PingResponse::node).forEach(discoveredNodes::add);
                    }
                    return new PeersResponse(optionalMasterNode, discoveredNodes, 0L);
                }, UnicastZenPing.UnicastPingResponse::new);
            } else {
                actionName = REQUEST_PEERS_ACTION_NAME;
                transportRequest = new PeersRequest(getLocalNode(), knownNodes);
                transportResponseHandler = peersResponseHandler;
            }
            // 发送请求到远程节点
            transportService.sendRequest(discoveryNode, actionName,
                transportRequest,
                TransportRequestOptions.builder().withTimeout(requestPeersTimeout).build(),
                transportResponseHandler);
        }

        @Override
        public String toString() {
            return "Peer{" +
                "transportAddress=" + transportAddress +
                ", discoveryNode=" + discoveryNode.get() +
                ", peersRequestInFlight=" + peersRequestInFlight +
                '}';
        }
    }

    /**
     * 用于处理Zen1单播Ping请求的内部类。
     */
    private class Zen1UnicastPingRequestHandler implements TransportRequestHandler<UnicastZenPing.UnicastPingRequest> {
        /**
         * 当接收到Zen1单播Ping请求时调用。
         * @param request 接收到的单播Ping请求。
         * @param channel 传输通道。
         * @param task 当前任务。
         * @throws Exception 可能抛出的异常。
         */
        @Override
        public void messageReceived(UnicastZenPing.UnicastPingRequest request, TransportChannel channel, Task task) throws Exception {
            // 根据Ping请求创建Peers请求
            final PeersRequest peersRequest = new PeersRequest(
                request.pingResponse.node(), // 使用Ping响应中的节点
                Optional.ofNullable(request.pingResponse.master()) // 如果Ping响应中有主节点
                    .map(Collections::singletonList) // 将其转换为单元素列表
                    .orElse(emptyList()) // 否则使用空列表
            );

            // 处理Peers请求并获取响应
            final PeersResponse peersResponse = handlePeersRequest(peersRequest);

            // 准备Ping响应列表
            final List<ZenPing.PingResponse> pingResponses = new ArrayList<>();
            final ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings); // 获取集群名称设置

            // 添加本地节点的Ping响应
            pingResponses.add(new ZenPing.PingResponse(
                createDiscoveryNodeWithImpossiblyHighId(transportService.getLocalNode()), // 使用本地节点
                peersResponse.getMasterNode().orElse(null), // 使用Peers响应中的主节点
                clusterName, // 集群名称
                ClusterState.UNKNOWN_VERSION // 未知的集群状态版本
            ));

            // 对Peers响应中已知节点列表的每个节点添加Ping响应
            peersResponse.getKnownPeers().forEach(dn -> pingResponses.add(
                new ZenPing.PingResponse(
                    ZenPing.PingResponse.FAKE_PING_ID, // 使用假的Ping ID
                    isZen1Node(dn) ? dn : createDiscoveryNodeWithImpossiblyHighId(dn), // 根据是否是Zen1节点选择节点
                    null, // 未使用的字段
                    clusterName, // 集群名称
                    ClusterState.UNKNOWN_VERSION // 集群状态版本
                )
            ));

            // 发送Ping响应数组作为单播Ping响应
            channel.sendResponse(new UnicastZenPing.UnicastPingResponse(
                request.id, // 使用请求的ID
                pingResponses.toArray(new ZenPing.PingResponse[0]) // 将Ping响应列表转换为数组
            ));
        }
    }

}
