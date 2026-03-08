package com.bengangllipson.kafkaclient

import com.bengangllipson.kafkaclient.consumer.Consumer
import com.bengangllipson.kafkaclient.flow.FilteredMessage
import com.bengangllipson.kafkaclient.flow.State
import com.bengangllipson.kafkaclient.flow.Success
import com.bengangllipson.kafkaclient.model.Payload
import com.bengangllipson.kafkaclient.model.ProcessingStep
import com.bengangllipson.kafkaclient.model.WorkerConfiguration
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import kotlin.math.absoluteValue


@Serializable
data class ParsedPayload(
    val tcin: String,
    val locationId: String,
    val onHandQuantity: List<Event>,
    val onPurchaseQuantity: List<Event>,
    val onTransferQuantity: List<Event>
) {
    @Serializable
    data class Event(
        val quantity: Int
    )
}

data class TransformedResult(
    val tcin: String, val locationId: String, val quantity: Quantity
) {
    data class Quantity(
        val onHand: Int, val onPurchase: Int, val onTransfer: Int
    )
}

class Example {
    val pipeline: suspend (ProcessingStep<Payload>) -> ProcessingStep<State<TransformedResult>> =
        { (metadata, payload) ->

            val valid = payload.key.split(":").firstOrNull()?.toIntOrNull() != null

            if (!valid) {
                metadata to FilteredMessage
            }
            try {
                val parsed = Json.decodeFromString<ParsedPayload>(
                    payload.body.decodeToString()
                )

                val result = TransformedResult(
                    tcin = parsed.tcin,
                    locationId = parsed.locationId,
                    quantity = TransformedResult.Quantity(
                        onHand = parsed.onHandQuantity.sumOf { it.quantity },
                        onPurchase = parsed.onPurchaseQuantity.sumOf { it.quantity },
                        onTransfer = parsed.onTransferQuantity.sumOf { it.quantity })
                )

                metadata to Success(result)

            } catch (_: Exception) {
                metadata to FilteredMessage
            }
        }

    val commitStrategy: suspend (ProcessingStep<State<TransformedResult>>, KafkaConsumer<String, ByteArray>) -> Unit =
        { (metadata, state), consumer ->
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
        println("Consumer error: ${throwable.message}")
        throwable.printStackTrace()
    }

    val consumer = Consumer.Builder(
        config = Consumer.Config(
            appName = "example",
            broker = "localhost:9092",
            topics = listOf("my-topic"),
            group = "example-group",
            autoOffsetResetConfig = "earliest",
            workerConfiguration = WorkerConfiguration(
                count = 100, mailboxSize = 5000, selector = { (metadata, _) ->
                    metadata.key.hashCode().absoluteValue.rem(100)
                }),
        ), pipeline = pipeline, commitStrategy = commitStrategy, onError = onError
    ).build()

    fun start() {
        consumer.start()
    }
}

fun main() {
    Example().start()
}