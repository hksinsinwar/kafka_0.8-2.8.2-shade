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
import org.apache.kafka.common.serialization.StringDeserializer;

final class V282KafkaConsumerAdapter implements UnifiedKafkaConsumer {
    private final Consumer<String, String> consumer;

    V282KafkaConsumerAdapter(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    static V282KafkaConsumerAdapter fromConfig(KafkaClientConfig config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getBootstrapServers());
        properties.put("client.id", config.getClientId());
        properties.put("group.id", config.getGroupId());
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.putAll(config.getExtraProperties());
        return new V282KafkaConsumerAdapter(new KafkaConsumer<String, String>(properties));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
    }

    @Override
    public List<KafkaRecord> poll(long timeoutMs) {
        ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(timeoutMs));
        List<KafkaRecord> records = new ArrayList<KafkaRecord>();
        for (ConsumerRecord<String, String> record : polled) {
            records.add(new KafkaRecord(record.topic(), record.key(), record.value()));
        }
        return records;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
