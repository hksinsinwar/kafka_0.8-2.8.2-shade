package com.example.kafkashade;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class KafkaClientFactoryTest {

    @Test
    void createsV282ClientsWhenVersionSelected() {
        KafkaClientConfig config = KafkaClientConfig.builder(KafkaVersion.V2_8_2)
            .bootstrapServers("localhost:9092")
            .groupId("g1")
            .build();

        UnifiedKafkaProducer producer = KafkaClientFactory.createProducer(config);
        UnifiedKafkaConsumer consumer = KafkaClientFactory.createConsumer(config);

        assertTrue(producer instanceof V282KafkaProducerAdapter);
        assertTrue(consumer instanceof V282KafkaConsumerAdapter);

        producer.close();
        consumer.close();
    }

    @Test
    void createsV08ClientsWhenVersionSelected() {
        KafkaClientConfig config = KafkaClientConfig.builder(KafkaVersion.V0_8)
            .bootstrapServers("localhost:9092")
            .zookeeperConnect("localhost:2181")
            .groupId("g1")
            .build();

        UnifiedKafkaProducer producer = KafkaClientFactory.createProducer(config);
        UnifiedKafkaConsumer consumer = KafkaClientFactory.createConsumer(config);

        assertTrue(producer instanceof V08KafkaProducerAdapter);
        assertTrue(consumer instanceof V08KafkaConsumerAdapter);

        producer.close();
        consumer.close();
    }
}
