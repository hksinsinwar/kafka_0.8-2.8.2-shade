package com.example.kafkashade;

public final class KafkaRecord<K, V> {
    private final String topic;
    private final K key;
    private final V value;

    public KafkaRecord(String topic, K key, V value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
