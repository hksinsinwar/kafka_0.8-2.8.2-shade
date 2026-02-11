package com.example.kafkashade;

public interface ByteSerializer<T> {
    byte[] serialize(T value);
}
