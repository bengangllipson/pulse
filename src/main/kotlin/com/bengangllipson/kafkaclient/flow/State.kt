package com.bengangllipson.kafkaclient.flow

sealed interface State<out T>

@JvmInline
value class Success<T>(val value: T) : State<T>
data object FilteredMessage : State<Nothing>
