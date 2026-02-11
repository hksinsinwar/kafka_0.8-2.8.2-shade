package com.example.kafkashade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
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
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Test
    void producerDelegatesSendWithSerializedBytes() {
        @SuppressWarnings("unchecked")
        Producer<byte[], byte[]> delegate = (Producer<byte[], byte[]>) mock(Producer.class);
        V282KafkaProducerAdapter<String, String> adapter = new V282KafkaProducerAdapter<String, String>(
            delegate,
            KafkaSerDes.utf8String(),
            KafkaSerDes.utf8String()
        );

        adapter.send("t", "k", "v");

        verify(delegate).send(any());
    }

    @Test
    void consumerConvertsPolledRecordsFromBytes() {
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> delegate = (Consumer<byte[], byte[]>) mock(Consumer.class);
        ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord<byte[], byte[]>(
            "topic",
            0,
            0L,
            "k1".getBytes(UTF8),
            "v1".getBytes(UTF8)
        );
        TopicPartition tp = new TopicPartition("topic", 0);
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<byte[], byte[]>(
            Collections.singletonMap(tp, Arrays.asList(rec))
        );

        when(delegate.poll(any())).thenReturn(records);

        V282KafkaConsumerAdapter<String, String> adapter = new V282KafkaConsumerAdapter<String, String>(
            delegate,
            KafkaSerDes.utf8String(),
            KafkaSerDes.utf8String()
        );
        List<KafkaRecord<String, String>> got = adapter.poll(100L);

        assertEquals(1, got.size());
        assertEquals("topic", got.get(0).getTopic());
        assertEquals("k1", got.get(0).getKey());
        assertEquals("v1", got.get(0).getValue());
    }
}
