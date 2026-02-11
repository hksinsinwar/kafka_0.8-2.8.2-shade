package com.example.kafkashade;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

final class V282KafkaProducerAdapter<K, V> implements UnifiedKafkaProducer<K, V> {
    private final Producer<byte[], byte[]> producer;
    private final KafkaSerDe<K> keySerDe;
    private final KafkaSerDe<V> valueSerDe;

    V282KafkaProducerAdapter(
        Producer<byte[], byte[]> producer,
        KafkaSerDe<K> keySerDe,
        KafkaSerDe<V> valueSerDe
    ) {
        this.producer = producer;
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
    }

    static <K, V> V282KafkaProducerAdapter<K, V> fromConfig(KafkaClientConfig<K, V> config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.putAll(config.getExtraProperties());
        properties.put("key.serializer", ByteArraySerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        return new V282KafkaProducerAdapter<K, V>(
            new KafkaProducer<byte[], byte[]>(properties),
            config.getKeySerDe(),
            config.getValueSerDe()
        );
    }

    @Override
    public void send(String topic, K key, V value) {
        byte[] rawKey = keySerDe.serialize(key);
        byte[] rawValue = valueSerDe.serialize(value);
        producer.send(new ProducerRecord<byte[], byte[]>(topic, rawKey, rawValue));
    }

    @Override
    public void close() {
        producer.close();
    }
}
