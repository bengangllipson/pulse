package com.bengangllipson.pulse.flow

import com.bengangllipson.pulse.model.WorkerConfiguration
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
        a = Int.MAX_VALUE, b = workers.count * workers.mailboxSize
    ), workerProcessor: suspend (T) -> R
): Flow<R> = channelFlow {
    coroutineScope {
        val workerMailboxes = (0 until workers.count).map {
            Channel<Pair<T, CompletableDeferred<R>>>(capacity = workers.mailboxSize)
        }
        val outbox = produce(capacity = capacity) {
            this@parallel.collect {
                val deferred = CompletableDeferred<R>()
                workerMailboxes[workers.selector(it)].send(
                    element = it to deferred
                )
                send(element = deferred)
            }
            workerMailboxes.forEach { it.close() }
        }
        workerMailboxes.forEach { mailbox ->
            launch {
                mailbox.consumeEach { (value, deferred) ->
                    deferred.complete(value = workerProcessor(value))
                }
            }
        }
        outbox.consumeEach { deferred ->
            this@channelFlow.send(element = deferred.await())
        }
    }
}
