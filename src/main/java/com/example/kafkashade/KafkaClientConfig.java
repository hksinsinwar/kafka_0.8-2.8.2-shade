package com.example.kafkashade;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class KafkaClientConfig<K, V> {
    private final KafkaVersion version;
    private final String bootstrapServers;
    private final String zookeeperConnect;
    private final String clientId;
    private final String groupId;
    private final Map<String, String> extraProperties;
    private final KafkaSerDe<K> keySerDe;
    private final KafkaSerDe<V> valueSerDe;

    private KafkaClientConfig(Builder<K, V> builder) {
        this.version = builder.version;
        this.bootstrapServers = builder.bootstrapServers;
        this.zookeeperConnect = builder.zookeeperConnect;
        this.clientId = builder.clientId;
        this.groupId = builder.groupId;
        this.extraProperties = Collections.unmodifiableMap(new HashMap<String, String>(builder.extraProperties));
        this.keySerDe = builder.keySerDe;
        this.valueSerDe = builder.valueSerDe;
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

    public KafkaSerDe<K> getKeySerDe() {
        return keySerDe;
    }

    public KafkaSerDe<V> getValueSerDe() {
        return valueSerDe;
    }

    public static <K, V> Builder<K, V> builder(KafkaVersion version) {
        return new Builder<K, V>(version);
    }

    public static final class Builder<K, V> {

        private final KafkaVersion version;
        private String bootstrapServers = "localhost:9092";
        private String zookeeperConnect = "localhost:2181";
        private String clientId = "kafka-shaded-bridge";
        private String groupId = "kafka-shaded-bridge-group";
        private final Map<String, String> extraProperties = new HashMap<String, String>();
        private KafkaSerDe<K> keySerDe;
        private KafkaSerDe<V> valueSerDe;

        private Builder(KafkaVersion version) {
            this.version = version;
            this.keySerDe = defaultBytesSerDe();
            this.valueSerDe = defaultBytesSerDe();
        }

        @SuppressWarnings("unchecked")
        private static <T> KafkaSerDe<T> defaultBytesSerDe() {
            return (KafkaSerDe<T>) KafkaSerDes.bytes();
        }

        public Builder<K, V> bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder<K, V> zookeeperConnect(String zookeeperConnect) {
            this.zookeeperConnect = zookeeperConnect;
            return this;
        }

        public Builder<K, V> clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder<K, V> groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder<K, V> put(String key, String value) {
            this.extraProperties.put(key, value);
            return this;
        }

        public Builder<K, V> keySerDe(KafkaSerDe<K> keySerDe) {
            this.keySerDe = keySerDe;
            return this;
        }

        public Builder<K, V> valueSerDe(KafkaSerDe<V> valueSerDe) {
            this.valueSerDe = valueSerDe;
            return this;
        }

        public KafkaClientConfig<K, V> build() {
            return new KafkaClientConfig<K, V>(this);
        }
    }
}
