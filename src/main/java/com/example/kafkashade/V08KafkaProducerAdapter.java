package com.example.kafkashade;

import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

final class V08KafkaProducerAdapter implements UnifiedKafkaProducer {
    private final Producer<String, String> producer;

    V08KafkaProducerAdapter(Producer<String, String> producer) {
        this.producer = producer;
    }

    static V08KafkaProducerAdapter fromConfig(KafkaClientConfig config) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        for (Map.Entry<String, String> entry : config.getExtraProperties().entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        ProducerConfig producerConfig = new ProducerConfig(properties);
        return new V08KafkaProducerAdapter(new Producer<String, String>(producerConfig));
    }

    @Override
    public void send(String topic, String key, String value) {
        producer.send(new KeyedMessage<String, String>(topic, key, value));
    }

    @Override
    public void close() {
        producer.close();
    }
}
