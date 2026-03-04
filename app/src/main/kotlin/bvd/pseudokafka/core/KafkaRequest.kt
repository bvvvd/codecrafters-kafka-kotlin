package bvd.pseudokafka.core

import java.nio.ByteBuffer

data class KafkaRequest(
    val messageSize: Int,
    val apiKey: Short,
    val apiVersion: Short,
    val correlationId: Int,
    val topicName: String? = null,
) {
    companion object {
        fun from(buffer: ByteBuffer): KafkaRequest {
            val messageSize = buffer.int
            val apiKey = buffer.short
            val apiVersion = buffer.short
            val correlationId = buffer.int

            return KafkaRequest(
                messageSize = messageSize,
                apiKey = apiKey,
                apiVersion = apiVersion,
                correlationId = correlationId,
                topicName = extractTopicName(buffer, apiKey),
            )
        }

        private fun extractTopicName(buffer: ByteBuffer, apiKey: Short): String? {
            if (apiKey != KafkaResponse.DESCRIBE_TOPIC_PARTITIONS_KEY || !buffer.hasRemaining()) {
                return null
            }

            val requestBuffer = buffer.slice()
            skipNullableString(requestBuffer)
            skipTaggedFields(requestBuffer)

            val topicCount = readUnsignedVarInt(requestBuffer) - 1
            if (topicCount <= 0) {
                return null
            }

            val topicName = readCompactString(requestBuffer)
            skipTaggedFields(requestBuffer)
            return topicName
        }

        private fun skipNullableString(buffer: ByteBuffer) {
            if (buffer.remaining() < 2) {
                return
            }

            val length = buffer.short.toInt()
            if (length < 0) {
                return
            }

            skipBytes(buffer, length)
        }

        private fun readCompactString(buffer: ByteBuffer): String? {
            val length = readUnsignedVarInt(buffer)
            if (length <= 1) {
                return null
            }

            val byteCount = length - 1
            if (buffer.remaining() < byteCount) {
                return null
            }

            val bytes = ByteArray(byteCount)
            buffer.get(bytes)
            return String(bytes)
        }

        private fun skipTaggedFields(buffer: ByteBuffer) {
            val taggedFieldCount = readUnsignedVarInt(buffer)
            repeat(taggedFieldCount) {
                readUnsignedVarInt(buffer)
                val size = readUnsignedVarInt(buffer)
                skipBytes(buffer, size)
            }
        }

        private fun skipBytes(buffer: ByteBuffer, count: Int) {
            val safeCount = minOf(count, buffer.remaining())
            buffer.position(buffer.position() + safeCount)
        }

        private fun readUnsignedVarInt(buffer: ByteBuffer): Int {
            var value = 0
            var shift = 0

            while (buffer.hasRemaining()) {
                val current = buffer.get().toInt() and 0xFF
                value = value or ((current and 0x7F) shl shift)
                if ((current and 0x80) == 0) {
                    return value
                }
                shift += 7
            }

            return value
        }
    }
}
