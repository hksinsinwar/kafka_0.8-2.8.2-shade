package com.example.kafkashade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

final class V08KafkaConsumerAdapter<K, V> implements UnifiedKafkaConsumer<K, V> {

    interface LegacyConsumer<K, V> {
        void subscribe(Collection<String> topics);

        List<KafkaRecord<K, V>> poll(long timeoutMs);

        void close();
    }

    static final class Kafka08LegacyConsumer<K, V> implements LegacyConsumer<K, V> {
        private final ConsumerConnector consumerConnector;
        private final KafkaSerDe<K> keySerDe;
        private final KafkaSerDe<V> valueSerDe;
        private final Map<String, List<KafkaStream<byte[], byte[]>>> streamsByTopic =
            new HashMap<String, List<KafkaStream<byte[], byte[]>>>();

        Kafka08LegacyConsumer(
            ConsumerConnector consumerConnector,
            KafkaSerDe<K> keySerDe,
            KafkaSerDe<V> valueSerDe
        ) {
            this.consumerConnector = consumerConnector;
            this.keySerDe = keySerDe;
            this.valueSerDe = valueSerDe;
        }

        static <K, V> Kafka08LegacyConsumer<K, V> fromConfig(KafkaClientConfig<K, V> config) {
            Properties properties = new Properties();
            properties.put("zookeeper.connect", config.getZookeeperConnect());
            properties.put("group.id", config.getGroupId());
            properties.put("consumer.id", config.getClientId());
            properties.put("consumer.timeout.ms", "250");
            properties.putAll(config.getExtraProperties());
            ConsumerConfig consumerConfig = new ConsumerConfig(properties);
            return new Kafka08LegacyConsumer<K, V>(
                Consumer.createJavaConsumerConnector(consumerConfig),
                config.getKeySerDe(),
                config.getValueSerDe()
            );
        }

        @Override
        public void subscribe(Collection<String> topics) {
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            for (String topic : topics) {
                topicCountMap.put(topic, Integer.valueOf(1));
            }
            streamsByTopic.clear();
            streamsByTopic.putAll(consumerConnector.createMessageStreams(topicCountMap));
        }

        @Override
        public List<KafkaRecord<K, V>> poll(long timeoutMs) {
            List<KafkaRecord<K, V>> records = new ArrayList<KafkaRecord<K, V>>();
            for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streamsByTopic.entrySet()) {
                String topic = entry.getKey();
                for (KafkaStream<byte[], byte[]> stream : entry.getValue()) {
                    ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
                    while (true) {
                        try {
                            MessageAndMetadata<byte[], byte[]> message = iterator.next();
                            K key = keySerDe.deserialize(message.key());
                            V value = valueSerDe.deserialize(message.message());
                            records.add(new KafkaRecord<K, V>(topic, key, value));
                        } catch (ConsumerTimeoutException timeout) {
                            break;
                        }
                    }
                }
            }
            return records;
        }

        @Override
        public void close() {
            consumerConnector.shutdown();
        }
    }

    private final LegacyConsumer<K, V> consumer;

    V08KafkaConsumerAdapter(LegacyConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    static <K, V> V08KafkaConsumerAdapter<K, V> fromConfig(KafkaClientConfig<K, V> config) {
        return new V08KafkaConsumerAdapter<K, V>(Kafka08LegacyConsumer.fromConfig(config));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics == null ? Collections.<String>emptyList() : topics);
    }

    @Override
    public List<KafkaRecord<K, V>> poll(long timeoutMs) {
        return consumer.poll(timeoutMs);
    }

    @Override
    public void close() {
        consumer.close();
    }
}
