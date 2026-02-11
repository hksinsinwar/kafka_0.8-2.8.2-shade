package com.example.kafkashade;

public final class KafkaClientFactory {
    private KafkaClientFactory() {
    }

    public static <K, V> UnifiedKafkaProducer<K, V> createProducer(KafkaClientConfig<K, V> config) {
        if (config.getVersion() == KafkaVersion.V0_8) {
            return V08KafkaProducerAdapter.fromConfig(config);
        }
        return V282KafkaProducerAdapter.fromConfig(config);
    }

    public static <K, V> UnifiedKafkaConsumer<K, V> createConsumer(KafkaClientConfig<K, V> config) {
        if (config.getVersion() == KafkaVersion.V0_8) {
            return V08KafkaConsumerAdapter.fromConfig(config);
        }
        return V282KafkaConsumerAdapter.fromConfig(config);
    }
}
