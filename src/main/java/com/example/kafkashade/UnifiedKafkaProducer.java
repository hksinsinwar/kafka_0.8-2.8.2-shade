package com.example.kafkashade;

public interface UnifiedKafkaProducer extends AutoCloseable {
    void send(String topic, String key, String value);

    @Override
    void close();
}
