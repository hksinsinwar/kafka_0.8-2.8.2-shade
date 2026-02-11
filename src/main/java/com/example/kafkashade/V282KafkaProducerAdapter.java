package com.example.kafkashade;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

final class V282KafkaProducerAdapter implements UnifiedKafkaProducer {
    private final Producer<String, String> producer;

    V282KafkaProducerAdapter(Producer<String, String> producer) {
        this.producer = producer;
    }

    static V282KafkaProducerAdapter fromConfig(KafkaClientConfig config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.putAll(config.getExtraProperties());
        return new V282KafkaProducerAdapter(new KafkaProducer<String, String>(properties));
    }

    @Override
    public void send(String topic, String key, String value) {
        producer.send(new ProducerRecord<String, String>(topic, key, value));
    }

    @Override
    public void close() {
        producer.close();
    }
}
