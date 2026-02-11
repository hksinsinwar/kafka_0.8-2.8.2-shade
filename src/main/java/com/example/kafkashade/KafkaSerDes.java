package com.example.kafkashade;

import java.nio.charset.Charset;

public final class KafkaSerDes {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private KafkaSerDes() {
    }

    public static KafkaSerDe<byte[]> bytes() {
        return new KafkaSerDe<byte[]>(
            new ByteSerializer<byte[]>() {
                @Override
                public byte[] serialize(byte[] value) {
                    return value;
                }
            },
            new ByteDeserializer<byte[]>() {
                @Override
                public byte[] deserialize(byte[] data) {
                    return data;
                }
            }
        );
    }

    public static KafkaSerDe<String> utf8String() {
        return new KafkaSerDe<String>(
            new ByteSerializer<String>() {
                @Override
                public byte[] serialize(String value) {
                    return value == null ? null : value.getBytes(UTF8);
                }
            },
            new ByteDeserializer<String>() {
                @Override
                public String deserialize(byte[] data) {
                    return data == null ? null : new String(data, UTF8);
                }
            }
        );
    }
}
