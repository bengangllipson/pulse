package com.bengangllipson.pulse.producer

import com.bengangllipson.pulse.Handle
import com.bengangllipson.pulse.flow.parallel
import com.bengangllipson.pulse.model.ProcessingStep
import com.bengangllipson.pulse.model.WorkerConfiguration
import com.bengangllipson.pulse.serde.JacksonSerializer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class Producer<T>(
    val config: Config<T>,
    val recordMapper: (T) -> ProducerRecord<String, T>,
    val onError: (Throwable) -> Unit
) {
    data class Config<T>(
        val appName: String,
        val broker: String,
        val topic: String,
        val driverProperties: Map<String, String>? = emptyMap(),
        val valueSerializer: Class<out Serializer<*>> = JacksonSerializer::class.java,
        val workerConfig: WorkerConfiguration<T>,
    )

    private fun createProducer(): KafkaProducer<String, T> {
        val props = Properties().apply {
            this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = config.broker
            this[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
            this[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = config.valueSerializer.name
            this[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = 1000L
            this[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "lz4"
            this[ProducerConfig.ACKS_CONFIG] = "all"
            this[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = 1024 * 1024 * 4
            this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = 1
            this[ProducerConfig.BATCH_SIZE_CONFIG] = 1024 * 851
            this[ProducerConfig.BUFFER_MEMORY_CONFIG] = 1024 * 1024 * 64
            this[ProducerConfig.LINGER_MS_CONFIG] = 100
            config.driverProperties?.entries?.forEach {
                this[it.key] = it.value
            }
        }
        return KafkaProducer(props)
    }

    private suspend fun sendRecord(
        producer: KafkaProducer<String, T>, record: ProducerRecord<String, T>
    ): RecordMetadata = suspendCancellableCoroutine { cancellableContinuation ->
        val future = producer.send(record) { metadata, exception ->
            if (exception != null) {
                if (!cancellableContinuation.isCompleted) cancellableContinuation.resumeWith(
                    Result.failure(
                        exception
                    )
                )
            } else {
                if (!cancellableContinuation.isCompleted) cancellableContinuation.resumeWith(Result.success(metadata))
            }
        }
        cancellableContinuation.invokeOnCancellation {
            try {
                future.cancel(true)
            } catch (t: Throwable) {
                onError(t)
            }
        }
    }

    fun start(inputFlow: Flow<T>): Handle {
        val producer = createProducer()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val job = scope.launch {
            try {
                inputFlow.parallel(config.workerConfig) { element ->
                    sendRecord(producer, recordMapper(element))
                }.collect {}
            } catch (t: Throwable) {
                onError(t)
            } finally {
                try {
                    withContext(producerThread) {
                        producer.flush()
                        producer.close()
                    }
                } catch (t: Throwable) {
                    onError(t)
                }
            }
        }

        val stop: suspend () -> Unit = {
            job.cancelAndJoin()
            withContext(producerThread) {
                producer.flush()
                producer.close()
            }
            scope.cancel()
        }

        return Handle(job = job, stop = stop)
    }
}
