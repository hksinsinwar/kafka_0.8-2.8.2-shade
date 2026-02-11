package com.example.kafkashade;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class KafkaClientConfig {
    private final KafkaVersion version;
    private final String bootstrapServers;
    private final String zookeeperConnect;
    private final String clientId;
    private final String groupId;
    private final Map<String, String> extraProperties;

    private KafkaClientConfig(Builder builder) {
        this.version = builder.version;
        this.bootstrapServers = builder.bootstrapServers;
        this.zookeeperConnect = builder.zookeeperConnect;
        this.clientId = builder.clientId;
        this.groupId = builder.groupId;
        this.extraProperties = Collections.unmodifiableMap(new HashMap<String, String>(builder.extraProperties));
    }

    public KafkaVersion getVersion() {
        return version;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public String getClientId() {
        return clientId;
    }

    public String getGroupId() {
        return groupId;
    }

    public Map<String, String> getExtraProperties() {
        return extraProperties;
    }

    public static Builder builder(KafkaVersion version) {
        return new Builder(version);
    }

    public static final class Builder {
        private final KafkaVersion version;
        private String bootstrapServers = "localhost:9092";
        private String zookeeperConnect = "localhost:2181";
        private String clientId = "kafka-shaded-bridge";
        private String groupId = "kafka-shaded-bridge-group";
        private final Map<String, String> extraProperties = new HashMap<String, String>();

        private Builder(KafkaVersion version) {
            this.version = version;
        }

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder zookeeperConnect(String zookeeperConnect) {
            this.zookeeperConnect = zookeeperConnect;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder put(String key, String value) {
            this.extraProperties.put(key, value);
            return this;
        }

        public KafkaClientConfig build() {
            return new KafkaClientConfig(this);
        }
    }
}
