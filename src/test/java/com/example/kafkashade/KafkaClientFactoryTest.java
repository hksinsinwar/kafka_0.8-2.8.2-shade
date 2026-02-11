package com.example.kafkashade;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class KafkaClientFactoryTest {

    @Test
    void createsV282ClientsWhenVersionSelected() {
        KafkaClientConfig<String, String> config = KafkaClientConfig.<String, String>builder(KafkaVersion.V2_8_2)
            .bootstrapServers("localhost:9092")
            .groupId("g1")
            .build();

        UnifiedKafkaProducer<String, String> producer = KafkaClientFactory.createProducer(config);
        UnifiedKafkaConsumer<String, String> consumer = KafkaClientFactory.createConsumer(config);

        assertTrue(producer instanceof V282KafkaProducerAdapter);
        assertTrue(consumer instanceof V282KafkaConsumerAdapter);

        producer.close();
        consumer.close();
    }

    @Test
    void createsV08ClientsWhenVersionSelected() {
        KafkaClientConfig<String, String> config = KafkaClientConfig.<String, String>builder(KafkaVersion.V0_8)
            .bootstrapServers("localhost:9092")
            .zookeeperConnect("localhost:2181")
            .groupId("g1")
            .build();

        UnifiedKafkaProducer<String, String> producer = KafkaClientFactory.createProducer(config);
        UnifiedKafkaConsumer<String, String> consumer = KafkaClientFactory.createConsumer(config);

        assertTrue(producer instanceof V08KafkaProducerAdapter);
        assertTrue(consumer instanceof V08KafkaConsumerAdapter);

        producer.close();
        consumer.close();
    }
}
