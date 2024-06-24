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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CoordinationMetaData implements Writeable, ToXContentFragment {

    public static final CoordinationMetaData EMPTY_META_DATA = builder().build();

    private final long term;

    private final VotingConfiguration lastCommittedConfiguration;

    private final VotingConfiguration lastAcceptedConfiguration;

    private final Set<VotingConfigExclusion> votingConfigExclusions;

    private static final ParseField TERM_PARSE_FIELD = new ParseField("term");
    private static final ParseField LAST_COMMITTED_CONFIGURATION_FIELD = new ParseField("last_committed_config");
    private static final ParseField LAST_ACCEPTED_CONFIGURATION_FIELD = new ParseField("last_accepted_config");
    private static final ParseField VOTING_CONFIG_EXCLUSIONS_FIELD = new ParseField("voting_config_exclusions");

    private static long term(Object[] termAndConfigs) {
        return (long)termAndConfigs[0];
    }

    @SuppressWarnings("unchecked")
    private static VotingConfiguration lastCommittedConfig(Object[] fields) {
        List<String> nodeIds = (List<String>) fields[1];
        return new VotingConfiguration(new HashSet<>(nodeIds));
    }

    @SuppressWarnings("unchecked")
    private static VotingConfiguration lastAcceptedConfig(Object[] fields) {
        List<String> nodeIds = (List<String>) fields[2];
        return new VotingConfiguration(new HashSet<>(nodeIds));
    }

    @SuppressWarnings("unchecked")
    private static Set<VotingConfigExclusion> votingConfigExclusions(Object[] fields) {
        Set<VotingConfigExclusion> votingTombstones = new HashSet<>((List<VotingConfigExclusion>) fields[3]);
        return votingTombstones;
    }

    private static final ConstructingObjectParser<CoordinationMetaData, Void> PARSER = new ConstructingObjectParser<>(
            "coordination_metadata",
            fields -> new CoordinationMetaData(term(fields), lastCommittedConfig(fields),
                    lastAcceptedConfig(fields), votingConfigExclusions(fields)));
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TERM_PARSE_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_COMMITTED_CONFIGURATION_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LAST_ACCEPTED_CONFIGURATION_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), VotingConfigExclusion.PARSER, VOTING_CONFIG_EXCLUSIONS_FIELD);
    }

    public CoordinationMetaData(long term, VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
                                Set<VotingConfigExclusion> votingConfigExclusions) {
        this.term = term;
        this.lastCommittedConfiguration = lastCommittedConfiguration;
        this.lastAcceptedConfiguration = lastAcceptedConfiguration;
        this.votingConfigExclusions = Collections.unmodifiableSet(new HashSet<>(votingConfigExclusions));
    }

    public CoordinationMetaData(StreamInput in) throws IOException {
        term = in.readLong();
        lastCommittedConfiguration = new VotingConfiguration(in);
        lastAcceptedConfiguration = new VotingConfiguration(in);
        votingConfigExclusions = Collections.unmodifiableSet(in.readSet(VotingConfigExclusion::new));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(CoordinationMetaData coordinationMetaData) {
        return new Builder(coordinationMetaData);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(term);
        lastCommittedConfiguration.writeTo(out);
        lastAcceptedConfiguration.writeTo(out);
        out.writeCollection(votingConfigExclusions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .field(TERM_PARSE_FIELD.getPreferredName(), term)
            .field(LAST_COMMITTED_CONFIGURATION_FIELD.getPreferredName(), lastCommittedConfiguration)
            .field(LAST_ACCEPTED_CONFIGURATION_FIELD.getPreferredName(), lastAcceptedConfiguration)
            .field(VOTING_CONFIG_EXCLUSIONS_FIELD.getPreferredName(), votingConfigExclusions);
    }

    public static CoordinationMetaData fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public long term() {
        return term;
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return lastAcceptedConfiguration;
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return lastCommittedConfiguration;
    }

    public Set<VotingConfigExclusion> getVotingConfigExclusions() {
        return votingConfigExclusions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CoordinationMetaData)) return false;

        CoordinationMetaData that = (CoordinationMetaData) o;

        if (term != that.term) return false;
        if (!lastCommittedConfiguration.equals(that.lastCommittedConfiguration)) return false;
        if (!lastAcceptedConfiguration.equals(that.lastAcceptedConfiguration)) return false;
        return votingConfigExclusions.equals(that.votingConfigExclusions);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + lastCommittedConfiguration.hashCode();
        result = 31 * result + lastAcceptedConfiguration.hashCode();
        result = 31 * result + votingConfigExclusions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CoordinationMetaData{" +
            "term=" + term +
            ", lastCommittedConfiguration=" + lastCommittedConfiguration +
            ", lastAcceptedConfiguration=" + lastAcceptedConfiguration +
            ", votingConfigExclusions=" + votingConfigExclusions +
            '}';
    }

    public static class Builder {
        private long term = 0;
        private VotingConfiguration lastCommittedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private VotingConfiguration lastAcceptedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private final Set<VotingConfigExclusion> votingConfigExclusions = new HashSet<>();

        public Builder() {

        }

        public Builder(CoordinationMetaData state) {
            this.term = state.term;
            this.lastCommittedConfiguration = state.lastCommittedConfiguration;
            this.lastAcceptedConfiguration = state.lastAcceptedConfiguration;
            this.votingConfigExclusions.addAll(state.votingConfigExclusions);
        }

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder lastCommittedConfiguration(VotingConfiguration config) {
            this.lastCommittedConfiguration = config;
            return this;
        }

        public Builder lastAcceptedConfiguration(VotingConfiguration config) {
            this.lastAcceptedConfiguration = config;
            return this;
        }

        public Builder addVotingConfigExclusion(VotingConfigExclusion exclusion) {
            votingConfigExclusions.add(exclusion);
            return this;
        }

        public Builder clearVotingConfigExclusions() {
            votingConfigExclusions.clear();
            return this;
        }

        public CoordinationMetaData build() {
            return new CoordinationMetaData(term, lastCommittedConfiguration, lastAcceptedConfiguration, votingConfigExclusions);
        }
    }

    public static class VotingConfigExclusion implements Writeable, ToXContentFragment {
        private final String nodeId;
        private final String nodeName;

        public VotingConfigExclusion(DiscoveryNode node) {
            this(node.getId(), node.getName());
        }

        public VotingConfigExclusion(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.nodeName = in.readString();
        }

        public VotingConfigExclusion(String nodeId, String nodeName) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(nodeName);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getNodeName() {
            return nodeName;
        }

        private static final ParseField NODE_ID_PARSE_FIELD = new ParseField("node_id");
        private static final ParseField NODE_NAME_PARSE_FIELD = new ParseField("node_name");

        private static String nodeId(Object[] nodeIdAndName) {
            return (String) nodeIdAndName[0];
        }

        private static String nodeName(Object[] nodeIdAndName) {
            return (String) nodeIdAndName[1];
        }

        private static final ConstructingObjectParser<VotingConfigExclusion, Void> PARSER = new ConstructingObjectParser<>(
                "voting_config_exclusion",
                nodeIdAndName -> new VotingConfigExclusion(nodeId(nodeIdAndName), nodeName(nodeIdAndName))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_PARSE_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_NAME_PARSE_FIELD);
        }

        public static VotingConfigExclusion fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field(NODE_ID_PARSE_FIELD.getPreferredName(), nodeId)
                    .field(NODE_NAME_PARSE_FIELD.getPreferredName(), nodeName)
                    .endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VotingConfigExclusion that = (VotingConfigExclusion) o;
            return Objects.equals(nodeId, that.nodeId) &&
                    Objects.equals(nodeName, that.nodeName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, nodeName);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (nodeName.length() > 0) {
                sb.append('{').append(nodeName).append('}');
            }
            sb.append('{').append(nodeId).append('}');
            return sb.toString();
        }

    }

    /**
     * 表示集群状态变更的投票配置，包含一组持久化的节点ID集合。
     */
    public static class VotingConfiguration implements Writeable, ToXContentFragment {

        /**
         * 空的投票配置。
         */
        public static final VotingConfiguration EMPTY_CONFIG = new VotingConfiguration(Collections.emptySet());
        /**
         * 一个特殊的投票配置，表示节点必须加入选举出的主节点。
         */
        public static final VotingConfiguration MUST_JOIN_ELECTED_MASTER = new VotingConfiguration(Collections.singleton(
            "_must_join_elected_master_"));

        private final Set<String> nodeIds; // 存储节点ID的集合

        public VotingConfiguration(Set<String> nodeIds) {
            // 构造函数，使用不可修改的集合包装传入的节点ID集合
            this.nodeIds = Collections.unmodifiableSet(new HashSet<>(nodeIds));
        }

        public VotingConfiguration(StreamInput in) throws IOException {
            // 反序列化构造函数，从输入流中读取节点ID集合
            nodeIds = Collections.unmodifiableSet(Sets.newHashSet(in.readStringArray()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // 序列化方法，将节点ID数组写入输出流
            out.writeStringArray(nodeIds.toArray(new String[nodeIds.size()]));
        }

        /**
         * 检查是否有足够的投票达到法定人数。
         * @param votes 投票的集合
         * @return 如果达到法定人数返回true，否则返回false
         */
        public boolean hasQuorum(Collection<String> votes) {
            final HashSet<String> intersection = new HashSet<>(nodeIds);
            intersection.retainAll(votes);
            return intersection.size() * 2 > nodeIds.size();
        }

        public Set<String> getNodeIds() {
            // 返回节点ID集合
            return nodeIds;
        }

        @Override
        public String toString() {
            // 返回节点ID集合的字符串表示
            return "VotingConfiguration{" + String.join(",", nodeIds) + "}";
        }

        // equals, hashCode 方法的实现，基于节点ID集合

        /**
         * 判断投票配置是否为空。
         * @return 如果节点ID集合为空返回true，否则返回false
         */
        public boolean isEmpty() {
            return nodeIds.isEmpty();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // 将节点ID集合转换为XContent格式
            builder.startArray();
            for (String nodeId : nodeIds) {
                builder.value(nodeId);
            }
            return builder.endArray();
        }

        /**
         * 根据一组DiscoveryNode对象创建VotingConfiguration实例。
         * @param nodes DiscoveryNode数组
         * @return VotingConfiguration实例
         */
        public static VotingConfiguration of(DiscoveryNode... nodes) {
            // 从DiscoveryNode数组中提取节点ID，并创建VotingConfiguration实例
            return new VotingConfiguration(Arrays.stream(nodes).map(DiscoveryNode::getId).collect(Collectors.toSet()));
        }
    }
}
