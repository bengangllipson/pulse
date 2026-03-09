@file:OptIn(ExperimentalCoroutinesApi::class)

package com.bengangllipson.pulse.consumer

import com.bengangllipson.pulse.Handle
import com.bengangllipson.pulse.flow.State
import com.bengangllipson.pulse.flow.parallel
import com.bengangllipson.pulse.model.InputMetadata
import com.bengangllipson.pulse.model.Payload
import com.bengangllipson.pulse.model.ProcessingStep
import com.bengangllipson.pulse.model.WorkerConfiguration
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class Consumer<O>(
    val config: Config,
    val pipeline: suspend (ProcessingStep<Payload>) -> ProcessingStep<State<O>>,
    val commitStrategy: suspend (ProcessingStep<State<O>>, KafkaConsumer<String, ByteArray>) -> Unit,
    val onError: (Throwable) -> Unit
) {
    data class Config(
        val appName: String,
        val broker: String,
        val topics: List<String>,
        val group: String,
        val pollTimeoutMs: Long = 1000L,
        val maxPollRecords: Int = 500,
        val autoOffsetResetConfig: String,
        val maxCommitErrors: Int = 10,
        val driverProperties: Map<String, String>? = emptyMap(),
        val workerConfig: WorkerConfiguration<ProcessingStep<Payload>>,
    )

    private fun createConsumer(): KafkaConsumer<String, ByteArray> {
        val props = Properties().apply {
            this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = config.broker
            this[ConsumerConfig.GROUP_ID_CONFIG] = config.group
            this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
            this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer().javaClass
            this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer().javaClass
            this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = config.autoOffsetResetConfig
            config.driverProperties?.entries?.forEach {
                this[it.key] = it.value
            }
        }
        return KafkaConsumer(props)
    }

    fun start(): Handle {
        val consumer = createConsumer()
        consumer.subscribe(config.topics)
        val scope = CoroutineScope(context = SupervisorJob() + Dispatchers.Default)
        val job = scope.launch {
            try {
                while (isActive) {
                    val records = withContext(consumerThread) {
                        consumer.poll(Duration.ofMillis(config.pollTimeoutMs))
                    } as ConsumerRecords<String, ByteArray>

                    val batchSize = records.count()
                    if (batchSize == 0) {
                        delay(timeMillis = 100L)
                        continue
                    }

                    val kafkaMessages = channelFlow {
                        records.forEachIndexed { index, record ->
                            val key = record.key()
                            val body = record.value()
                            val meta = InputMetadata(
                                topic = record.topic(),
                                key = key,
                                partitionOffset = Pair(record.partition(), record.offset()),
                                isEndOfBatch = index + 1 == batchSize
                            )
                            send(element = meta to Payload(key = key, body = body))
                        }
                    }.flowOn(context = consumerThread)

                    kafkaMessages.parallel(workers = config.workerConfig) { step ->
                        pipeline(step)
                    }.collect { processedStep ->
                        try {
                            commitStrategy(processedStep, consumer)
                        } catch (e: Throwable) {
                            onError(e)
                        }
                    }
                }
            } catch (t: Throwable) {
                onError(t)
            } finally {
                consumer.close()
            }
        }

        val stop: suspend () -> Unit = {
            job.cancelAndJoin()
            try {
                consumer.close()
            } catch (t: Throwable) {
                onError(t)
            }
            scope.cancel()
        }

        return Handle(job = job, stop = stop)
    }
}
