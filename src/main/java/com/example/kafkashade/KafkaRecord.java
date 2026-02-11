package com.example.kafkashade;

public final class KafkaRecord {
    private final String topic;
    private final String key;
    private final String value;

    public KafkaRecord(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
