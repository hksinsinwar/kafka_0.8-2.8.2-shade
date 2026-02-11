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

final class V282KafkaConsumerAdapter<K, V> implements UnifiedKafkaConsumer<K, V> {
    private final Consumer<K, V> consumer;

    V282KafkaConsumerAdapter(Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    static <K, V> V282KafkaConsumerAdapter<K, V> fromConfig(KafkaClientConfig<K, V> config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.put("group.id", config.getGroupId());
        properties.put("auto.offset.reset", "earliest");
        properties.putAll(config.getExtraProperties());
        return new V282KafkaConsumerAdapter<K, V>(new KafkaConsumer<K, V>(properties));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
    }

    @Override
    public List<KafkaRecord<K, V>> poll(long timeoutMs) {
        ConsumerRecords<K, V> polled = consumer.poll(Duration.ofMillis(timeoutMs));
        List<KafkaRecord<K, V>> records = new ArrayList<KafkaRecord<K, V>>();
        for (ConsumerRecord<K, V> record : polled) {
            records.add(new KafkaRecord<K, V>(record.topic(), record.key(), record.value()));
        }
        return records;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
