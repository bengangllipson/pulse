package com.bengangllipson.kafkaclient.model

data class WorkerConfiguration<M>(
    val count: Int, val mailboxSize: Int, val selector: (M) -> Int
)
