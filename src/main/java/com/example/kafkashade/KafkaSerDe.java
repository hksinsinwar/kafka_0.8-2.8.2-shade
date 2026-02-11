package com.example.kafkashade;

public final class KafkaSerDe<T> {
    private final ByteSerializer<T> serializer;
    private final ByteDeserializer<T> deserializer;

    public KafkaSerDe(ByteSerializer<T> serializer, ByteDeserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public byte[] serialize(T value) {
        return serializer.serialize(value);
    }

    public T deserialize(byte[] data) {
        return deserializer.deserialize(data);
    }
}
