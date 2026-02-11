package com.example.kafkashade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import kafka.javaapi.producer.Producer;
import org.junit.jupiter.api.Test;

class V08KafkaAdapterTest {

    @Test
    void producerDelegatesSend() {
        @SuppressWarnings("unchecked")
        Producer<String, String> delegate = (Producer<String, String>) mock(Producer.class);
        V08KafkaProducerAdapter<String, String> adapter = new V08KafkaProducerAdapter<String, String>(delegate);

        adapter.send("topic", "key", "value");

        verify(delegate).send(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void consumerUsesLegacyDelegate() {
        V08KafkaConsumerAdapter.LegacyConsumer<String, String> legacyConsumer =
            new V08KafkaConsumerAdapter.LegacyConsumer<String, String>() {
                @Override
                public void subscribe(java.util.Collection<String> topics) {
                }

                @Override
                public List<KafkaRecord<String, String>> poll(long timeoutMs) {
                    return Collections.singletonList(new KafkaRecord<String, String>("legacy", "k", "v"));
                }

                @Override
                public void close() {
                }
            };

        V08KafkaConsumerAdapter<String, String> adapter =
            new V08KafkaConsumerAdapter<String, String>(legacyConsumer);
        adapter.subscribe(Arrays.asList("legacy"));
        List<KafkaRecord<String, String>> records = adapter.poll(50L);

        assertEquals(1, records.size());
        assertEquals("legacy", records.get(0).getTopic());
    }
}
