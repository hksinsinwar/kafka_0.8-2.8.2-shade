package com.example.kafkashade;

import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

final class V08KafkaProducerAdapter<K, V> implements UnifiedKafkaProducer<K, V> {
    private final Producer<K, V> producer;

    V08KafkaProducerAdapter(Producer<K, V> producer) {
        this.producer = producer;
    }

    static <K, V> V08KafkaProducerAdapter<K, V> fromConfig(KafkaClientConfig<K, V> config) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        for (Map.Entry<String, String> entry : config.getExtraProperties().entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        ProducerConfig producerConfig = new ProducerConfig(properties);
        return new V08KafkaProducerAdapter<K, V>(new Producer<K, V>(producerConfig));
    }

    @Override
    public void send(String topic, K key, V value) {
        producer.send(new KeyedMessage<K, V>(topic, key, value));
    }

    @Override
    public void close() {
        producer.close();
    }
}
