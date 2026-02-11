# Kafka 0.8 + 2.8.2 shaded bridge (Java 8 / Maven)

This project provides a single Java 8 generic API (`<K,V>`) that can drive **Kafka 0.8** and **Kafka 2.8.2** clients.

## What this solves

- Supports producer and consumer abstractions for both versions:
  - `KafkaVersion.V0_8`
  - `KafkaVersion.V2_8_2`
- Uses `byte[]` as the internal transport payload for both client versions.
- Abstracts serialization/deserialization via wrappers (`KafkaSerDe`) so clients keep generic types while adapters send/receive bytes.
- Creates a shaded artifact so both Kafka dependency trees can coexist.
- Lets users switch Kafka version at runtime via `KafkaClientConfig`.

## Build requirements

- Java 8
- Maven 3.8+

## Quick setup

```bash
./setup.sh
```

## Build and test

```bash
mvn clean test
mvn clean package
```

The shaded jar is generated as:

- `target/kafka-shaded-bridge-1.0.0-SNAPSHOT-all.jar`

## Usage

### Default `byte[]` mode (no custom serde needed)

```java
KafkaClientConfig<byte[], byte[]> config = KafkaClientConfig.<byte[], byte[]>builder(KafkaVersion.V2_8_2)
    .bootstrapServers("localhost:9092")
    .groupId("my-group")
    .build();
```

### Generic mode with serde wrappers

```java
KafkaClientConfig<String, String> config = KafkaClientConfig.<String, String>builder(KafkaVersion.V2_8_2)
    .bootstrapServers("localhost:9092")
    .groupId("my-group")
    .keySerDe(KafkaSerDes.utf8String())
    .valueSerDe(KafkaSerDes.utf8String())
    .build();

UnifiedKafkaProducer<String, String> producer = KafkaClientFactory.createProducer(config);
producer.send("topic-a", "k1", "hello");
producer.close();

UnifiedKafkaConsumer<String, String> consumer = KafkaClientFactory.createConsumer(config);
consumer.subscribe(Arrays.asList("topic-a"));
List<KafkaRecord<String, String>> records = consumer.poll(1000L);
consumer.close();
```


For Kafka 2.8.2 typed use, set deserializer/serializer classes through `put(...)` (for example `key.serializer`, `value.serializer`, `key.deserializer`, `value.deserializer`).

For Kafka 0.8 consumers, configure ZooKeeper:

```java
KafkaClientConfig<String, String> legacyConfig = KafkaClientConfig.<String, String>builder(KafkaVersion.V0_8)
    .bootstrapServers("localhost:9092")
    .zookeeperConnect("localhost:2181")
    .groupId("legacy-group")
    .keySerDe(KafkaSerDes.utf8String())
    .valueSerDe(KafkaSerDes.utf8String())
    .build();
```

## Project layout

- `KafkaClientFactory` – version switch / adapter factory
- `UnifiedKafkaProducer` / `UnifiedKafkaConsumer` – common abstractions
- `KafkaSerDe`, `ByteSerializer`, `ByteDeserializer`, `KafkaSerDes` – generic serializer/deserializer wrapper layer
- `V08*Adapter` – Kafka 0.8 adapter implementations (byte transport + serde wrappers)
- `V282*Adapter` – Kafka 2.8.2 adapter implementations (byte transport + serde wrappers)
- `V08*Adapter` – Kafka 0.8 adapter implementations
- `V282*Adapter` – Kafka 2.8.2 adapter implementations

## Notes

- Kafka 0.8 consumer API depends on ZooKeeper.
- This project uses unit tests with mocked delegates instead of spinning up brokers.
