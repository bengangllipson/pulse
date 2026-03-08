@file:OptIn(ExperimentalCoroutinesApi::class)

package com.bengangllipson.kafkaclient

import com.bengangllipson.kafkaclient.consumer.ConsumerHandle
import com.bengangllipson.kafkaclient.flow.State
import com.bengangllipson.kafkaclient.flow.parallel
import com.bengangllipson.kafkaclient.model.InputMetadata
import com.bengangllipson.kafkaclient.model.Payload
import com.bengangllipson.kafkaclient.model.ProcessingStep
import com.bengangllipson.kafkaclient.model.WorkerConfiguration
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
import java.util.concurrent.Executors

private val consumerThread = Executors.newSingleThreadExecutor { r ->
    Thread(r, "kafka-consumer")
}.asCoroutineDispatcher()

class Consumer<P, R>(
    private val config: Config,
    private val pipeline: suspend (ProcessingStep<Payload>) -> ProcessingStep<State<R>>,
    private val commitStrategy: suspend (ProcessingStep<State<R>>, KafkaConsumer<String, ByteArray>) -> Unit,
    private val onError: (Throwable) -> Unit
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
        val workerConfiguration: WorkerConfiguration<ProcessingStep<Payload>>,
    )

    private fun createConsumer(): KafkaConsumer<String, ByteArray> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.broker)
            put(ConsumerConfig.GROUP_ID_CONFIG, config.group)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer().javaClass)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer().javaClass)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetResetConfig)
        }
        return KafkaConsumer(props)
    }

    fun start(): ConsumerHandle {
        val consumer = createConsumer()
        consumer.subscribe(config.topics)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val job = scope.launch {
            try {
                while (isActive) {
                    val records = withContext(consumerThread) {
                        consumer.poll(Duration.ofMillis(config.pollTimeoutMs))
                    } as ConsumerRecords<String, ByteArray>

                    val batchSize = records.count()
                    if (batchSize == 0) {
                        delay(100L)
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
                            send(meta to Payload(key = key, body = body))
                        }
                    }.flowOn(consumerThread)

                    kafkaMessages.parallel(config.workerConfiguration) { step ->
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
            } catch (_: Throwable) {
            }
            scope.cancel()
        }

        return ConsumerHandle(job = job, stop = stop)
    }

    data class Builder<P, R>(
        val config: Config,
        val pipeline: suspend (ProcessingStep<Payload>) -> ProcessingStep<State<R>>,
        val commitStrategy: suspend (ProcessingStep<State<R>>, KafkaConsumer<String, ByteArray>) -> Unit,
        val onError: (Throwable) -> Unit
    ) {
        fun build(): Consumer<P, R> {
            return Consumer(
                config = config,
                pipeline = pipeline,
                commitStrategy = commitStrategy,
                onError = onError
            )
        }
    }
}
