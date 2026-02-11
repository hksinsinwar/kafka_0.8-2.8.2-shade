package com.example.kafkashade;

import java.util.Collection;
import java.util.List;

public interface UnifiedKafkaConsumer<K, V> extends AutoCloseable {
    void subscribe(Collection<String> topics);

    List<KafkaRecord<K, V>> poll(long timeoutMs);

    @Override
    void close();
}
