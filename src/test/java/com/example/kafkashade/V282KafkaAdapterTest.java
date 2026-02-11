package com.example.kafkashade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class V282KafkaAdapterTest {

    @Test
    void producerDelegatesSend() {
        @SuppressWarnings("unchecked")
        Producer<String, String> delegate = (Producer<String, String>) mock(Producer.class);
        V282KafkaProducerAdapter<String, String> adapter = new V282KafkaProducerAdapter<String, String>(delegate);

        adapter.send("t", "k", "v");

        verify(delegate).send(any());
    }

    @Test
    void consumerConvertsPolledRecords() {
        @SuppressWarnings("unchecked")
        Consumer<String, String> delegate = (Consumer<String, String>) mock(Consumer.class);
        ConsumerRecord<String, String> rec = new ConsumerRecord<String, String>("topic", 0, 0L, "k1", "v1");
        TopicPartition tp = new TopicPartition("topic", 0);
        ConsumerRecords<String, String> records = new ConsumerRecords<String, String>(
            Collections.singletonMap(tp, Arrays.asList(rec))
        );

        when(delegate.poll(any())).thenReturn(records);

        V282KafkaConsumerAdapter<String, String> adapter = new V282KafkaConsumerAdapter<String, String>(delegate);
        List<KafkaRecord<String, String>> got = adapter.poll(100L);

        assertEquals(1, got.size());
        assertEquals("topic", got.get(0).getTopic());
        assertEquals("k1", got.get(0).getKey());
        assertEquals("v1", got.get(0).getValue());
    }
}
