package bvd.pseudokafka.core

import java.nio.ByteBuffer

sealed interface KafkaResponse {
    fun toByteBuffer(): ByteBuffer

    data class ErrorResponse(
        val correlationId: Int,
        val errorCode: Short = UNSUPPORTED_VERSION_ERROR_CODE,
    ) : KafkaResponse {
        override fun toByteBuffer(): ByteBuffer {
            val response = ByteBuffer.allocate(10)
            response.putInt(6)
            response.putInt(correlationId)
            response.putShort(errorCode)
            response.flip()
            return response
        }
    }

    data class ApiVersionsResponse(
        val correlationId: Int,
        val apiVersions: List<ApiVersion>,
        val errorCode: Short = NO_ERROR_CODE,
        val throttleTimeMs: Int = 0,
    ) : KafkaResponse {
        override fun toByteBuffer(): ByteBuffer {
            val body = ByteBuffer.allocate(64)
            body.putShort(errorCode)
            writeUnsignedVarInt(body, apiVersions.size + 1)
            apiVersions.forEach { apiVersion ->
                body.putShort(apiVersion.apiKey)
                body.putShort(apiVersion.minVersion)
                body.putShort(apiVersion.maxVersion)
                body.put(NO_TAGGED_FIELDS)
            }
            body.putInt(throttleTimeMs)
            body.put(NO_TAGGED_FIELDS)

            val bodyLength = body.position()
            val response = ByteBuffer.allocate(4 + 4 + bodyLength)
            response.putInt(4 + bodyLength)
            response.putInt(correlationId)
            response.put(body.array(), 0, bodyLength)
            response.flip()
            return response
        }
    }

    data class DescribeTopicPartitionsResponse(val correlationId: Int, val topicPartitions: List<TopicPartition>) : KafkaResponse {
        data class TopicPartition(
            val topicName: String,
            val errorCode: Short = UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
        )

        override fun toByteBuffer(): ByteBuffer {
            val body = ByteBuffer.allocate(128)
            body.putInt(0)
            writeUnsignedVarInt(body, topicPartitions.size + 1)
            topicPartitions.forEach { topicPartition ->
                body.putShort(topicPartition.errorCode)
                writeCompactString(body, topicPartition.topicName)
                repeat(16) { body.put(0) }
                body.put(0)
                writeUnsignedVarInt(body, 1)
                body.putInt(0)
                body.put(NO_TAGGED_FIELDS)
            }
            body.put(NULL_CURSOR)
            body.put(NO_TAGGED_FIELDS)

            val bodyLength = body.position()
            val response = ByteBuffer.allocate(4 + 4 + 1 + bodyLength)
            response.putInt(4 + 1 + bodyLength)
            response.putInt(correlationId)
            response.put(NO_TAGGED_FIELDS)
            response.put(body.array(), 0, bodyLength)
            response.flip()
            return response
        }
    }

    data class ApiVersion(
        val apiKey: Short,
        val minVersion: Short,
        val maxVersion: Short,
    )

    companion object {
        const val API_VERSIONS_KEY: Short = 18
        const val DESCRIBE_TOPIC_PARTITIONS_KEY: Short = 75
        const val MIN_API_VERSION: Short = 0
        const val MAX_API_VERSION: Short = 4
        const val NO_ERROR_CODE: Short = 0
        const val UNSUPPORTED_VERSION_ERROR_CODE: Short = 35
        const val UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE: Short = 3
        const val NO_TAGGED_FIELDS: Byte = 0
        const val NULL_CURSOR: Byte = -1

        private fun writeCompactString(buffer: ByteBuffer, value: String) {
            val bytes = value.toByteArray()
            writeUnsignedVarInt(buffer, bytes.size + 1)
            buffer.put(bytes)
        }

        private fun writeUnsignedVarInt(buffer: ByteBuffer, value: Int) {
            var current = value
            while (true) {
                if ((current and 0x7F.inv()) == 0) {
                    buffer.put(current.toByte())
                    return
                }

                buffer.put(((current and 0x7F) or 0x80).toByte())
                current = current ushr 7
            }
        }
    }
}
