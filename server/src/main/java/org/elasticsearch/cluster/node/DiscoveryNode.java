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

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;


/**
 * 一个发现节点（DiscoveryNode）代表集群中的一个节点。
 * <p>
 * 此类用于存储有关集群节点的信息，包括节点的网络地址、唯一标识符、版本信息等。
 * 这些信息在节点发现、集群状态管理和节点间通信时使用。
 */
public class DiscoveryNode implements Writeable, ToXContentFragment {

    /**
     * 表示仅协调节点的常量字符串。
     */
    static final String COORDINATING_ONLY = "coordinating_only";

    /**
     * 判断节点是否需要本地存储。
     * @param settings 节点的设置。
     * @return 如果节点需要本地存储，则返回true。
     */
    public static boolean nodeRequiresLocalStorage(Settings settings) {
        // 获取节点本地存储设置的值
        boolean localStorageEnable = Node.NODE_LOCAL_STORAGE_SETTING.get(settings);
        // 如果本地存储被禁用，并且节点设置了数据或主节点角色，则抛出异常
        if (localStorageEnable == false &&
            (Node.NODE_DATA_SETTING.get(settings) ||
                Node.NODE_MASTER_SETTING.get(settings))
        ) {
            // 抛出异常，因为存储不能为数据节点或主节点禁用
            throw new IllegalArgumentException("storage can not be disabled for master and data nodes");
        }
        return localStorageEnable;
    }

    /**
     * 判断设置是否启用了主节点角色。
     * @param settings 节点的设置。
     * @return 如果启用了主节点角色，则返回true。
     */
    public static boolean isMasterNode(Settings settings) {
        return Node.NODE_MASTER_SETTING.get(settings);
    }

    /**
     * 判断设置是否启用了数据节点角色。
     * @param settings 节点的设置。
     * @return 如果启用了数据节点角色，则返回true。
     */
    public static boolean isDataNode(Settings settings) {
        return Node.NODE_DATA_SETTING.get(settings);
    }

    /**
     * 判断设置是否启用了摄入节点角色。
     * @param settings 节点的设置。
     * @return 如果启用了摄入节点角色，则返回true。
     */
    public static boolean isIngestNode(Settings settings) {
        return Node.NODE_INGEST_SETTING.get(settings);
    }

    /**
     * 节点名称。
     */
    private final String nodeName;

    /**
     * 节点ID。
     */
    private final String nodeId;

    /**
     * 节点的临时ID，用于在节点重新启动时保持一致性。
     */
    private final String ephemeralId;

    /**
     * 节点的主机名。
     */
    private final String hostName;

    /**
     * 节点的主机地址。
     */
    private final String hostAddress;

    /**
     * 节点的传输地址，用于节点间的通信。
     */
    private final TransportAddress address;

    /**
     * 节点的属性映射，包含节点特定的配置信息。
     */
    private final Map<String, String> attributes;

    /**
     * 节点运行的Elasticsearch版本。
     */
    private final Version version;

    /**
     * 节点的角色集合，如主节点、数据节点等。
     */
    private final Set<Role> roles;


    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
     * @param address          the nodes transport address
     * @param version          the version of the node
     */
    public DiscoveryNode(final String id, TransportAddress address, Version version) {
        this(id, address, Collections.emptyMap(), EnumSet.allOf(Role.class), version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     */
    public DiscoveryNode(String id, TransportAddress address, Map<String, String> attributes, Set<Role> roles,
                         Version version) {
        this("", id, address, attributes, roles, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName         the nodes name
     * @param nodeId           the nodes unique persistent id. An ephemeral id will be auto generated.
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     */
    public DiscoveryNode(String nodeName, String nodeId, TransportAddress address,
                         Map<String, String> attributes, Set<Role> roles, Version version) {
        this(nodeName, nodeId, UUIDs.randomBase64UUID(), address.address().getHostString(), address.getAddress(), address, attributes,
            roles, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}.
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName         the nodes name
     * @param nodeId           the nodes unique persistent id
     * @param ephemeralId      the nodes unique ephemeral id
     * @param hostAddress      the nodes host address
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     */
    public DiscoveryNode(String nodeName, String nodeId, String ephemeralId, String hostName, String hostAddress,
                         TransportAddress address, Map<String, String> attributes, Set<Role> roles, Version version) {
        if (nodeName != null) {
            this.nodeName = nodeName.intern();
        } else {
            this.nodeName = "";
        }
        this.nodeId = nodeId.intern();
        this.ephemeralId = ephemeralId.intern();
        this.hostName = hostName.intern();
        this.hostAddress = hostAddress.intern();
        this.address = address;
        if (version == null) {
            this.version = Version.CURRENT;
        } else {
            this.version = version;
        }
        this.attributes = Collections.unmodifiableMap(attributes);
        //verify that no node roles are being provided as attributes
        Predicate<Map<String, String>> predicate =  (attrs) -> {
            for (Role role : Role.values()) {
                assert attrs.containsKey(role.getRoleName()) == false;
            }
            return true;
        };
        assert predicate.test(attributes);
        Set<Role> rolesSet = EnumSet.noneOf(Role.class);
        rolesSet.addAll(roles);
        this.roles = Collections.unmodifiableSet(rolesSet);
    }

    /**
     * 创建一个代表本地节点的DiscoveryNode。
     * @param settings 节点的设置。
     * @param publishAddress 节点用于通信的传输地址。
     * @param nodeId 节点的唯一标识符。
     * @return 返回一个新的DiscoveryNode实例，代表本地节点。
     */
    public static DiscoveryNode createLocal(Settings settings, TransportAddress publishAddress, String nodeId) {
        // 从设置中获取节点属性
        Map<String, String> attributes = Node.NODE_ATTRIBUTES.getAsMap(settings);
        // 从设置中获取节点的角色
        Set<Role> roles = getRolesFromSettings(settings);
        // 创建并返回代表本地节点的DiscoveryNode实例
        return new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), nodeId, publishAddress, attributes, roles, Version.CURRENT);
    }

    /**
     * 从给定的设置中提取节点的角色。
     * @param settings 节点的设置。
     * @return 返回一个包含节点角色的集合。
     */
    public static Set<Role> getRolesFromSettings(Settings settings) {
        // 创建一个空的角色集合
        Set<Role> roles = EnumSet.noneOf(Role.class);
        // 检查摄入节点设置，并添加相应的角色
        if (Node.NODE_INGEST_SETTING.get(settings)) {
            roles.add(Role.INGEST);
        }
        // 检查主节点设置，并添加相应的角色
        if (Node.NODE_MASTER_SETTING.get(settings)) {
            roles.add(Role.MASTER);
        }
        // 检查数据节点设置，并添加相应的角色
        if (Node.NODE_DATA_SETTING.get(settings)) {
            roles.add(Role.DATA);
        }
        // 返回包含所有检测到的角色的集合
        return roles;
    }

    /**
     * Creates a new {@link DiscoveryNode} by reading from the stream provided as argument
     * @param in the stream
     * @throws IOException if there is an error while reading from the stream
     */
    public DiscoveryNode(StreamInput in) throws IOException {
        this.nodeName = in.readString().intern();
        this.nodeId = in.readString().intern();
        this.ephemeralId = in.readString().intern();
        this.hostName = in.readString().intern();
        this.hostAddress = in.readString().intern();
        this.address = new TransportAddress(in);
        int size = in.readVInt();
        this.attributes = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            this.attributes.put(in.readString(), in.readString());
        }
        int rolesSize = in.readVInt();
        this.roles = EnumSet.noneOf(Role.class);
        for (int i = 0; i < rolesSize; i++) {
            this.roles.add(in.readEnum(Role.class));
        }
        this.version = Version.readVersion(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        out.writeString(nodeId);
        out.writeString(ephemeralId);
        out.writeString(hostName);
        out.writeString(hostAddress);
        address.writeTo(out);
        out.writeVInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeVInt(roles.size());
        for (Role role : roles) {
            out.writeEnum(role);
        }
        Version.writeVersion(version, out);
    }

    /**
     * The address that the node can be communicated with.
     */
    public TransportAddress getAddress() {
        return address;
    }

    /**
     * The unique id of the node.
     */
    public String getId() {
        return nodeId;
    }

    /**
     * The unique ephemeral id of the node. Ephemeral ids are meant to be attached the life span
     * of a node process. When ever a node is restarted, it's ephemeral id is required to change (while it's {@link #getId()}
     * will be read from the data folder and will remain the same across restarts). Since all node attributes and addresses
     * are maintained during the life span of a node process, we can (and are) using the ephemeralId in
     * {@link DiscoveryNode#equals(Object)}.
     */
    public String getEphemeralId() {
        return ephemeralId;
    }

    /**
     * The name of the node.
     */
    public String getName() {
        return this.nodeName;
    }

    /**
     * The node attributes.
     */
    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    /**
     * Should this node hold data (shards) or not.
     */
    public boolean isDataNode() {
        return roles.contains(Role.DATA);
    }

    /**
     * Can this node become master or not.
     */
    public boolean isMasterNode() {
        return roles.contains(Role.MASTER);
    }

    /**
     * Returns a boolean that tells whether this an ingest node or not
     */
    public boolean isIngestNode() {
        return roles.contains(Role.INGEST);
    }

    /**
     * Returns a set of all the roles that the node fulfills.
     * If the node doesn't have any specific role, the set is returned empty, which means that the node is a coordinating only node.
     */
    public Set<Role> getRoles() {
        return roles;
    }

    public Version getVersion() {
        return this.version;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getHostAddress() {
        return this.hostAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DiscoveryNode that = (DiscoveryNode) o;

        return ephemeralId.equals(that.ephemeralId);
    }

    @Override
    public int hashCode() {
        // we only need to hash the id because it's highly unlikely that two nodes
        // in our system will have the same id but be different
        // This is done so that this class can be used efficiently as a key in a map
        return ephemeralId.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (nodeName.length() > 0) {
            sb.append('{').append(nodeName).append('}');
        }
        sb.append('{').append(nodeId).append('}');
        sb.append('{').append(ephemeralId).append('}');
        sb.append('{').append(hostName).append('}');
        sb.append('{').append(address).append('}');
        if (!attributes.isEmpty()) {
            sb.append(attributes);
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getId());
        builder.field("name", getName());
        builder.field("ephemeral_id", getEphemeralId());
        builder.field("transport_address", getAddress().toString());

        builder.startObject("attributes");
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    /**
     * Enum that holds all the possible roles that that a node can fulfill in a cluster.
     * Each role has its name and a corresponding abbreviation used by cat apis.
     */
    public enum Role {
        MASTER("master", "m"),
        DATA("data", "d"),
        INGEST("ingest", "i");

        private final String roleName;
        private final String abbreviation;

        Role(String roleName, String abbreviation) {
            this.roleName = roleName;
            this.abbreviation = abbreviation;
        }

        public String getRoleName() {
            return roleName;
        }

        public String getAbbreviation() {
            return abbreviation;
        }
    }
}
