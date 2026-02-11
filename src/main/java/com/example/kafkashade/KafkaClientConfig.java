package com.example.kafkashade;

import java.nio.charset.Charset;
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
    private final LegacyDecoder<K> legacyKeyDecoder;
    private final LegacyDecoder<V> legacyValueDecoder;

    private KafkaClientConfig(Builder<K, V> builder) {
        this.version = builder.version;
        this.bootstrapServers = builder.bootstrapServers;
        this.zookeeperConnect = builder.zookeeperConnect;
        this.clientId = builder.clientId;
        this.groupId = builder.groupId;
        this.extraProperties = Collections.unmodifiableMap(new HashMap<String, String>(builder.extraProperties));
        this.legacyKeyDecoder = builder.legacyKeyDecoder;
        this.legacyValueDecoder = builder.legacyValueDecoder;
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

    public LegacyDecoder<K> getLegacyKeyDecoder() {
        return legacyKeyDecoder;
    }

    public LegacyDecoder<V> getLegacyValueDecoder() {
        return legacyValueDecoder;
    }

    public static <K, V> Builder<K, V> builder(KafkaVersion version) {
        return new Builder<K, V>(version);
    }

    public static final class Builder<K, V> {
        private static final Charset UTF8 = Charset.forName("UTF-8");

        private final KafkaVersion version;
        private String bootstrapServers = "localhost:9092";
        private String zookeeperConnect = "localhost:2181";
        private String clientId = "kafka-shaded-bridge";
        private String groupId = "kafka-shaded-bridge-group";
        private final Map<String, String> extraProperties = new HashMap<String, String>();
        private LegacyDecoder<K> legacyKeyDecoder;
        private LegacyDecoder<V> legacyValueDecoder;

        private Builder(KafkaVersion version) {
            this.version = version;
            this.legacyKeyDecoder = new LegacyDecoder<K>() {
                @SuppressWarnings("unchecked")
                @Override
                public K decode(byte[] data) {
                    return (K) (data == null ? null : new String(data, UTF8));
                }
            };
            this.legacyValueDecoder = new LegacyDecoder<V>() {
                @SuppressWarnings("unchecked")
                @Override
                public V decode(byte[] data) {
                    return (V) (data == null ? null : new String(data, UTF8));
                }
            };
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

        public Builder<K, V> legacyKeyDecoder(LegacyDecoder<K> legacyKeyDecoder) {
            this.legacyKeyDecoder = legacyKeyDecoder;
            return this;
        }

        public Builder<K, V> legacyValueDecoder(LegacyDecoder<V> legacyValueDecoder) {
            this.legacyValueDecoder = legacyValueDecoder;
            return this;
        }

        public KafkaClientConfig<K, V> build() {
            return new KafkaClientConfig<K, V>(this);
        }
    }
}
