package com.bengangllipson.pulse.producer

import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

val producerThread =
    Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "kafka-producer") }.asCoroutineDispatcher()
