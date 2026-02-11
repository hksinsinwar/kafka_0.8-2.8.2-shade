package com.example.kafkashade;

import java.util.Collection;
import java.util.List;

public interface UnifiedKafkaConsumer extends AutoCloseable {
    void subscribe(Collection<String> topics);

    List<KafkaRecord> poll(long timeoutMs);

    @Override
    void close();
}
