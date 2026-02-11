package com.example.kafkashade;

public final class KafkaClientFactory {
    private KafkaClientFactory() {
    }

    public static UnifiedKafkaProducer createProducer(KafkaClientConfig config) {
        if (config.getVersion() == KafkaVersion.V0_8) {
            return V08KafkaProducerAdapter.fromConfig(config);
        }
        return V282KafkaProducerAdapter.fromConfig(config);
    }

    public static UnifiedKafkaConsumer createConsumer(KafkaClientConfig config) {
        if (config.getVersion() == KafkaVersion.V0_8) {
            return V08KafkaConsumerAdapter.fromConfig(config);
        }
        return V282KafkaConsumerAdapter.fromConfig(config);
    }
}
