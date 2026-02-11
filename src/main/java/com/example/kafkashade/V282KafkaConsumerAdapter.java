package com.example.kafkashade;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

final class V282KafkaConsumerAdapter<K, V> implements UnifiedKafkaConsumer<K, V> {
    private final Consumer<byte[], byte[]> consumer;
    private final KafkaSerDe<K> keySerDe;
    private final KafkaSerDe<V> valueSerDe;

    V282KafkaConsumerAdapter(
        Consumer<byte[], byte[]> consumer,
        KafkaSerDe<K> keySerDe,
        KafkaSerDe<V> valueSerDe
    ) {
        this.consumer = consumer;
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
    }

    static <K, V> V282KafkaConsumerAdapter<K, V> fromConfig(KafkaClientConfig<K, V> config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.put("group.id", config.getGroupId());
        properties.put("auto.offset.reset", "earliest");
        properties.putAll(config.getExtraProperties());
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        return new V282KafkaConsumerAdapter<K, V>(
            new KafkaConsumer<byte[], byte[]>(properties),
            config.getKeySerDe(),
            config.getValueSerDe()
        );
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
    }

    @Override
    public List<KafkaRecord<K, V>> poll(long timeoutMs) {
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(timeoutMs));
        List<KafkaRecord<K, V>> records = new ArrayList<KafkaRecord<K, V>>();
        for (ConsumerRecord<byte[], byte[]> record : polled) {
            K key = keySerDe.deserialize(record.key());
            V value = valueSerDe.deserialize(record.value());
            records.add(new KafkaRecord<K, V>(record.topic(), key, value));
        }
        return records;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
