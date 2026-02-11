# Kafka 0.8 + 2.8.2 shaded bridge (Java 8 / Maven)

This project provides a single Java 8 API that can drive **Kafka 0.8** and **Kafka 2.8.2** clients.

## What this solves

- Supports producer and consumer abstractions for both versions:
  - `KafkaVersion.V0_8`
  - `KafkaVersion.V2_8_2`
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

```java
KafkaClientConfig config = KafkaClientConfig.builder(KafkaVersion.V2_8_2)
    .bootstrapServers("localhost:9092")
    .groupId("my-group")
    .build();

UnifiedKafkaProducer producer = KafkaClientFactory.createProducer(config);
producer.send("topic-a", "k1", "hello");
producer.close();

UnifiedKafkaConsumer consumer = KafkaClientFactory.createConsumer(config);
consumer.subscribe(Arrays.asList("topic-a"));
List<KafkaRecord> records = consumer.poll(1000L);
consumer.close();
```

For Kafka 0.8 consumers, configure ZooKeeper:

```java
KafkaClientConfig legacyConfig = KafkaClientConfig.builder(KafkaVersion.V0_8)
    .bootstrapServers("localhost:9092")
    .zookeeperConnect("localhost:2181")
    .groupId("legacy-group")
    .build();
```

## Project layout

- `KafkaClientFactory` – version switch / adapter factory
- `UnifiedKafkaProducer` / `UnifiedKafkaConsumer` – common abstractions
- `V08*Adapter` – Kafka 0.8 adapter implementations
- `V282*Adapter` – Kafka 2.8.2 adapter implementations

## Notes

- Kafka 0.8 consumer API depends on ZooKeeper.
- This project uses unit tests with mocked delegates instead of spinning up brokers.
