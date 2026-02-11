package com.example.kafkashade;

public interface UnifiedKafkaProducer<K, V> extends AutoCloseable {
    void send(String topic, K key, V value);

    @Override
    void close();
}
