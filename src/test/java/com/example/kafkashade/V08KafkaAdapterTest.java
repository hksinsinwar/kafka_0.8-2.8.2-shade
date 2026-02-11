package com.example.kafkashade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import kafka.javaapi.producer.Producer;
import org.junit.jupiter.api.Test;

class V08KafkaAdapterTest {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Test
    void producerDelegatesSend() {
        @SuppressWarnings("unchecked")
        Producer<byte[], byte[]> delegate = (Producer<byte[], byte[]>) mock(Producer.class);
        V08KafkaProducerAdapter<String, String> adapter = new V08KafkaProducerAdapter<String, String>(
            delegate,
            KafkaSerDes.utf8String(),
            KafkaSerDes.utf8String()
        );

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
                    return Collections.singletonList(
                        new KafkaRecord<String, String>(
                            "legacy",
                            new String("k".getBytes(UTF8), UTF8),
                            new String("v".getBytes(UTF8), UTF8)
                        )
                    );
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
