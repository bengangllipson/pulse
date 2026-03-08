package com.bengangllipson.kafkaclient.model

data class Payload(
    val key: String, val body: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as Payload
        if (key != other.key) {
            return false
        }
        if (!body.contentEquals(other.body)) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + body.contentHashCode()
        return result
    }
}
