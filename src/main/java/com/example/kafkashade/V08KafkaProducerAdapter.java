package com.example.kafkashade;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

final class V08KafkaProducerAdapter<K, V> implements UnifiedKafkaProducer<K, V> {
    private final Producer<byte[], byte[]> producer;
    private final KafkaSerDe<K> keySerDe;
    private final KafkaSerDe<V> valueSerDe;

    V08KafkaProducerAdapter(
        Producer<byte[], byte[]> producer,
        KafkaSerDe<K> keySerDe,
        KafkaSerDe<V> valueSerDe
    ) {
        this.producer = producer;
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
    }

    static <K, V> V08KafkaProducerAdapter<K, V> fromConfig(KafkaClientConfig<K, V> config) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.putAll(config.getExtraProperties());
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        properties.put("key.serializer.class", "kafka.serializer.DefaultEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        return new V08KafkaProducerAdapter<K, V>(
            new Producer<byte[], byte[]>(producerConfig),
            config.getKeySerDe(),
            config.getValueSerDe()
        );
    }

    @Override
    public void send(String topic, K key, V value) {
        byte[] rawKey = keySerDe.serialize(key);
        byte[] rawValue = valueSerDe.serialize(value);
        producer.send(new KeyedMessage<byte[], byte[]>(topic, rawKey, rawValue));
    }

    @Override
    public void close() {
        producer.close();
    }
}
