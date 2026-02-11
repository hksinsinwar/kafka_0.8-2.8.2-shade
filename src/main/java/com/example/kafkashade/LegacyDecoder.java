package com.example.kafkashade;

public interface LegacyDecoder<T> {
    T decode(byte[] data);
}
