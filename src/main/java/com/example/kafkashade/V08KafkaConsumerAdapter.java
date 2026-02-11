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

final class V08KafkaConsumerAdapter implements UnifiedKafkaConsumer {

    interface LegacyConsumer {
        void subscribe(Collection<String> topics);

        List<KafkaRecord> poll(long timeoutMs);

        void close();
    }

    static final class Kafka08LegacyConsumer implements LegacyConsumer {
        private final ConsumerConnector consumerConnector;
        private final Map<String, List<KafkaStream<byte[], byte[]>>> streamsByTopic =
            new HashMap<String, List<KafkaStream<byte[], byte[]>>>();

        Kafka08LegacyConsumer(ConsumerConnector consumerConnector) {
            this.consumerConnector = consumerConnector;
        }

        static Kafka08LegacyConsumer fromConfig(KafkaClientConfig config) {
            Properties properties = new Properties();
            properties.put("zookeeper.connect", config.getZookeeperConnect());
            properties.put("group.id", config.getGroupId());
            properties.put("consumer.id", config.getClientId());
            properties.put("consumer.timeout.ms", "250");
            properties.putAll(config.getExtraProperties());
            ConsumerConfig consumerConfig = new ConsumerConfig(properties);
            return new Kafka08LegacyConsumer(Consumer.createJavaConsumerConnector(consumerConfig));
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
        public List<KafkaRecord> poll(long timeoutMs) {
            List<KafkaRecord> records = new ArrayList<KafkaRecord>();
            for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streamsByTopic.entrySet()) {
                String topic = entry.getKey();
                for (KafkaStream<byte[], byte[]> stream : entry.getValue()) {
                    ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
                    while (true) {
                        try {
                            MessageAndMetadata<byte[], byte[]> message = iterator.next();
                            String key = message.key() == null ? null : new String(message.key());
                            String value = message.message() == null ? null : new String(message.message());
                            records.add(new KafkaRecord(topic, key, value));
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

    private final LegacyConsumer consumer;

    V08KafkaConsumerAdapter(LegacyConsumer consumer) {
        this.consumer = consumer;
    }

    static V08KafkaConsumerAdapter fromConfig(KafkaClientConfig config) {
        return new V08KafkaConsumerAdapter(Kafka08LegacyConsumer.fromConfig(config));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics == null ? Collections.<String>emptyList() : topics);
    }

    @Override
    public List<KafkaRecord> poll(long timeoutMs) {
        return consumer.poll(timeoutMs);
    }

    @Override
    public void close() {
        consumer.close();
    }
}
