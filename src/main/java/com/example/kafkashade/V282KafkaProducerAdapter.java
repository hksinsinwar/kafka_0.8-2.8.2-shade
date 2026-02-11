package com.example.kafkashade;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

final class V282KafkaProducerAdapter<K, V> implements UnifiedKafkaProducer<K, V> {
    private final Producer<K, V> producer;

    V282KafkaProducerAdapter(Producer<K, V> producer) {
        this.producer = producer;
    }

    static <K, V> V282KafkaProducerAdapter<K, V> fromConfig(KafkaClientConfig<K, V> config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.putAll(config.getExtraProperties());
        return new V282KafkaProducerAdapter<K, V>(new KafkaProducer<K, V>(properties));
    }

    @Override
    public void send(String topic, K key, V value) {
        producer.send(new ProducerRecord<K, V>(topic, key, value));
    }

    @Override
    public void close() {
        producer.close();
    }
}
