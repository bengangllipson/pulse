package com.bengangllipson.kafkaclient.flow

import com.bengangllipson.kafkaclient.model.WorkerConfiguration
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlin.math.min

@OptIn(ExperimentalCoroutinesApi::class)
fun <T, R> Flow<T>.parallel(
    workers: WorkerConfiguration<T>, capacity: Int = min(
        Int.MAX_VALUE, workers.count * workers.mailboxSize
    ), workerProcessor: suspend (T) -> R
): Flow<R> = channelFlow {
    coroutineScope {
        val workerMailboxes = (0 until workers.count).map {
            Channel<Pair<T, CompletableDeferred<R>>>(workers.mailboxSize)
        }
        val outbox = produce(capacity = capacity) {
            this@parallel.collect {
                val deferred = CompletableDeferred<R>()
                workerMailboxes[workers.selector(it)].send(
                    it to deferred
                )
                send(deferred)
            }
            workerMailboxes.forEach { it.close() }
        }
        workerMailboxes.forEach { mailbox ->
            launch {
                mailbox.consumeEach { (value, deferred) ->
                    deferred.complete(workerProcessor(value))
                }
            }
        }
        outbox.consumeEach { deferred ->
            this@channelFlow.send(deferred.await())
        }
    }
}
