package com.example.kafkashade;

public interface ByteDeserializer<T> {
    T deserialize(byte[] data);
}
