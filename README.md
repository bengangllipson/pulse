# Pulse

A high performance engine for Apache Kafka. Pulse uses Kotlin coroutines nad flows to expose a simple structure to
compose parallelized, back-pressure aware concurrent data processing pipelines.

## Features
- Dedicated threads
- Deterministic thread routing
- Preserve ordering of related events
- Composable, parallelizable processing steps
- Control commit and error semantics
- Kafka config control
- Backpressure awareness

## Usage
### Types
- `Payload(key: String, body: ByteArray)` - simple key/body carrier 
- `InputMetadata(topic,key,partitionOffset,isEndOfBatch)` - consumer-only metadata used for commits 
- `ProcessingStep<T> = Pair<InputMetadata, T>` - pipeline element type for consumers 
- `WorkerConfiguration<T>` - `{ count, mailboxSize, selector }` for parallel thread routing

### Consumer
```kotlin
val consumerConfig = Consumer.Config(
    appName = "example",
    broker = "localhost:9092",
    topics = listOf("my-topic"),
    group = "example-group",
    autoOffsetResetConfig = "earliest",
    workerConfig = WorkerConfiguration(
        count = 100,
        mailboxSize = 5000,
        selector = { (metadata, _) ->
            metadata.key.hashCode().absoluteValue.rem(other = 100)
        }
    ),
)

fun step1(step: ProcessingStep<Payload>): ProcessingStep<State<Payload>> { ... }
fun step2(step: ProcessingStep<Payload>): ProcessingStep<State<Payload>> { ... }
fun step3(step: ProcessingStep<Payload>): ProcessingStep<State<Payload>> { ... }

val pipeline: suspend (ProcessingStep<Payload>) -> ProcessingStep<State<TransformedResult>> = { step ->
    step
        .let(block = ::step1)
        .let(block = ::step2)
        .let(block = ::step3)
        // etc...
}

val commitStrategy: suspend (ProcessingStep<State<TransformedResult>>, KafkaConsumer<String, ByteArray>) -> Unit = { (metadata, _), consumer ->
    if (metadata.isEndOfBatch) {
        val (partition, offset) = metadata.partitionOffset
        consumer.commitSync(
            mapOf(
                TopicPartition(metadata.topic, partition) to OffsetAndMetadata(offset + 1)
            )
        )
    }
}

val onError: (Throwable) -> Unit = { throwable ->
    throwable.printStackTrace()
}

val consumer = Consumer(
    config = consumerConfig,
    pipeline = pipeline,
    commitStrategy = commitStrategy,
    onError = onError
)

val handle = consumer.start()
// ...
handle.stop()
```

### Producer
```kotlin
val producerConfig = Producer.Config<MyEvent>(
    appName = "example",
    broker = "localhost:9092",
    topic = "my-topic",
    workerConfig = WorkerConfiguration(
        count = 100,
        mailboxSize = 5000,
        selector = { myEvent ->
            myEvent.itemId.hashCode().absoluteValue % 100
        }
    )
)


val recordMapper: (MyEvent) -> ProducerRecord<String, MyEvent> = { myEvent ->
    ProducerRecord(
        producerConfig.topic, myEvent.itemId, myEvent
    )
}

val onError: (Throwable) -> Unit = { throwable ->
    throwable.printStackTrace()
}

val producer = Producer(
    config = producerConfig,
    recordMapper = recordMapper,
    onError = onError
)

val input = flowOf(
    MyEvent(
        itemId = "1",
        locationId = "12",
        price = 14.99f
    ),
    MyEvent(
        itemId = "2",
        locationId = "13",
        price = 24.99f
    ),
    MyEvent(
        itemId = "3",
        locationId = "14",
        price = 9.99f
    ),
)

val handle = producer.start(input)
// ...
handle.stop()
```
