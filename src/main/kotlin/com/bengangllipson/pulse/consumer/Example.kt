package com.bengangllipson.pulse.consumer

import com.bengangllipson.pulse.flow.FilteredMessage
import com.bengangllipson.pulse.flow.State
import com.bengangllipson.pulse.flow.Success
import com.bengangllipson.pulse.model.Payload
import com.bengangllipson.pulse.model.ProcessingStep
import com.bengangllipson.pulse.model.WorkerConfiguration
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import kotlin.math.absoluteValue

@Serializable
data class ParsedPayload(
    val itemId: String,
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
    fun filter(step: ProcessingStep<Payload>): ProcessingStep<State<Payload>> {
        val (meta, payload) = step
        val valid = payload.key.split(":").firstOrNull()?.toIntOrNull() != null

        return if (valid) {
            meta to Success(payload)
        } else {
            meta to FilteredMessage
        }
    }

    fun parse(step: ProcessingStep<State<Payload>>): ProcessingStep<State<ParsedPayload>> {
        val (meta, state) = step
        return when (state) {
            is FilteredMessage -> meta to FilteredMessage
            is Success -> {
                try {
                    val parsed = Json.decodeFromString<ParsedPayload>(
                        state.value.body.decodeToString()
                    )
                    meta to Success(value = parsed)
                } catch (_: Exception) {
                    meta to FilteredMessage
                }
            }
        }
    }

    fun transform(step: ProcessingStep<State<ParsedPayload>>): ProcessingStep<State<TransformedResult>> {
        val (meta, state) = step
        return when (state) {
            is FilteredMessage -> meta to FilteredMessage
            is Success -> {
                val p = state.value
                val result = TransformedResult(
                    tcin = p.itemId,
                    locationId = p.locationId,
                    quantity = TransformedResult.Quantity(
                        onHand = p.onHandQuantity.sumOf { it.quantity },
                        onPurchase = p.onPurchaseQuantity.sumOf { it.quantity },
                        onTransfer = p.onTransferQuantity.sumOf { it.quantity })
                )
                meta to Success(value = result)
            }
        }
    }

    val pipeline: suspend (ProcessingStep<Payload>) -> ProcessingStep<State<TransformedResult>> = { step ->
        step.let(block = ::filter).let(block = ::parse).let(block = ::transform)
    }

    val commitStrategy: suspend (ProcessingStep<State<TransformedResult>>, KafkaConsumer<String, ByteArray>) -> Unit =
        { (metadata, _), consumer ->
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
        config = Consumer.Config(
            appName = "example",
            broker = "localhost:9092",
            topics = listOf("my-topic"),
            group = "example-group",
            autoOffsetResetConfig = "earliest",
            workerConfig = WorkerConfiguration(
                count = 100, mailboxSize = 5000, selector = { (metadata, _) ->
                    metadata.key.hashCode().absoluteValue.rem(other = 100)
                }),
        ), pipeline = pipeline, commitStrategy = commitStrategy, onError = onError
    )

    fun start() {
        consumer.start()
    }
}

fun main() {
    Example().start()
}
