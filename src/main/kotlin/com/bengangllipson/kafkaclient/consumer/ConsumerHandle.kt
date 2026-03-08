package com.bengangllipson.kafkaclient.consumer

import kotlinx.coroutines.Job

data class ConsumerHandle(
    val job: Job,
    val stop: suspend () -> Unit
)
