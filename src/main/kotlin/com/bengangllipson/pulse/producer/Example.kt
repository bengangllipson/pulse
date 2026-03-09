package com.bengangllipson.pulse.producer

import com.bengangllipson.pulse.model.WorkerConfiguration
import kotlinx.coroutines.flow.flowOf
import kotlinx.serialization.Serializable
import org.apache.kafka.clients.producer.ProducerRecord
import kotlin.math.absoluteValue

@Serializable
data class MyEvent(
    val itemId: String,
    val locationId: String,
    val price: Float
)

class Example {
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

    fun start() {
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
        producer.start(input)
    }
}

fun main() {
    Example().start()
}
