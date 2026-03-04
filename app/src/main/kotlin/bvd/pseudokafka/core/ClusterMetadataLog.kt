package bvd.pseudokafka.core

import java.io.File
import java.nio.ByteBuffer
import java.util.UUID

data class ClusterMetadata(
    val topicsByName: Map<String, ClusterTopicMetadata>,
) {
    fun hasTopic(topicName: String): Boolean = topicsByName.containsKey(topicName)
    fun topic(topicName: String): ClusterTopicMetadata? = topicsByName[topicName]
    fun topicId(topicName: String): UUID? = topic(topicName)?.topicId
}

data class ClusterTopicMetadata(
    val topicId: UUID,
    val partitions: List<ClusterPartitionMetadata>,
)

data class ClusterPartitionMetadata(
    val partitionIndex: Int,
    val leaderId: Int,
    val leaderEpoch: Int,
    val replicaNodes: List<Int>,
    val isrNodes: List<Int>,
)

class ClusterMetadataLog(
    filePath: String = DEFAULT_CLUSTER_METADATA_LOG_PATH,
) {
    private val logFile = File(filePath)

    fun isAvailable(): Boolean {
        return logFile.exists() && logFile.isFile
    }

    fun readRawBytes(): ByteArray {
        if (!isAvailable()) {
            return byteArrayOf()
        }
        return logFile.readBytes()
    }

    fun readText(): String {
        return readRawBytes().toString(Charsets.ISO_8859_1)
    }

    fun containsTopic(topicName: String): Boolean {
        return readMetadata().hasTopic(topicName)
    }

    fun readMetadata(candidateTopics: Iterable<String> = emptyList()): ClusterMetadata {
        val source = readRawBytes()
        if (source.isEmpty()) {
            return ClusterMetadata(emptyMap())
        }

        val topicIdsByName = extractTopicIdsByName(source)
        if (topicIdsByName.isEmpty()) {
            return ClusterMetadata(emptyMap())
        }

        val partitionsByTopicId = extractPartitionsByTopicId(source, topicIdsByName.values.toSet())
        val topics = topicIdsByName.mapValues { (_, topicId) ->
            ClusterTopicMetadata(
                topicId = topicId,
                partitions = partitionsByTopicId[topicId] ?: emptyList(),
            )
        }

        if (candidateTopics.none()) {
            return ClusterMetadata(topics)
        }

        val candidates = candidateTopics.toSet()
        return ClusterMetadata(topics.filterKeys { candidates.contains(it) })
    }

    fun topicId(topicName: String): UUID? {
        if (topicName.isBlank()) {
            return null
        }

        return readMetadata(listOf(topicName)).topicId(topicName)
    }

    fun topicMetadata(topicName: String): ClusterTopicMetadata? {
        if (topicName.isBlank()) {
            return null
        }

        return readMetadata(listOf(topicName)).topic(topicName)
    }

    private fun extractTopicIdsByName(source: ByteArray): Map<String, UUID> {
        val topicIdsByName = LinkedHashMap<String, UUID>()

        // TopicRecord payload contains: COMPACT_STRING name, UUID topic_id, TAG_BUFFER.
        for (nameStart in 1 until source.size) {
            val compactLength = source[nameStart - 1].toInt() and 0xFF
            if (compactLength <= 1 || compactLength > MAX_TOPIC_NAME_LENGTH + 1) {
                continue
            }

            val nameLength = compactLength - 1
            val uuidStart = nameStart + nameLength
            val tagBufferIndex = uuidStart + UUID_SIZE_BYTES
            if (tagBufferIndex >= source.size) {
                continue
            }

            val topicBytes = source.copyOfRange(nameStart, uuidStart)
            if (!isValidTopicName(topicBytes)) {
                continue
            }

            if ((source[tagBufferIndex].toInt() and 0xFF) != NO_TAGGED_FIELDS) {
                continue
            }

            val uuid = readUuid(source, uuidStart)
            if (uuid == ZERO_UUID) {
                continue
            }

            val topicName = topicBytes.toString(Charsets.UTF_8)
            topicIdsByName[topicName] = uuid
        }

        return topicIdsByName
    }

    private fun extractPartitionsByTopicId(
        source: ByteArray,
        topicIds: Set<UUID>,
    ): Map<UUID, List<ClusterPartitionMetadata>> {
        val located = mutableMapOf<UUID, MutableMap<Int, LocatedPartition>>()

        topicIds.forEach { topicId ->
            val topicIdBytes = toBytes(topicId)
            findAllOffsets(source, topicIdBytes).forEach { topicIdOffset ->
                parsePartitionAroundTopicId(source, topicIdOffset, partitionBeforeTopicId = true)?.let { parsed ->
                    storePartition(located, topicId, topicIdOffset, parsed)
                }

                parsePartitionAroundTopicId(source, topicIdOffset, partitionBeforeTopicId = false)?.let { parsed ->
                    storePartition(located, topicId, topicIdOffset, parsed)
                }
            }
        }

        return located.mapValues { (_, byPartitionId) ->
            byPartitionId.values
                .sortedBy { it.offset }
                .map { it.partition }
                .sortedBy { it.partitionIndex }
        }
    }

    private fun storePartition(
        located: MutableMap<UUID, MutableMap<Int, LocatedPartition>>,
        topicId: UUID,
        offset: Int,
        partition: ClusterPartitionMetadata,
    ) {
        val byPartition = located.getOrPut(topicId) { mutableMapOf() }
        val existing = byPartition[partition.partitionIndex]
        if (existing == null || offset >= existing.offset) {
            byPartition[partition.partitionIndex] = LocatedPartition(partition, offset)
        }
    }

    private fun parsePartitionAroundTopicId(
        source: ByteArray,
        topicIdOffset: Int,
        partitionBeforeTopicId: Boolean,
    ): ClusterPartitionMetadata? {
        val partitionIndexOffset = if (partitionBeforeTopicId) {
            topicIdOffset - INT_SIZE_BYTES
        } else {
            topicIdOffset + UUID_SIZE_BYTES
        }
        if (partitionIndexOffset < 0 || partitionIndexOffset + INT_SIZE_BYTES > source.size) {
            return null
        }

        val partitionIndex = readInt(source, partitionIndexOffset)
        if (partitionIndex !in 0..MAX_PARTITION_INDEX) {
            return null
        }

        val arraysOffset = if (partitionBeforeTopicId) {
            topicIdOffset + UUID_SIZE_BYTES
        } else {
            partitionIndexOffset + INT_SIZE_BYTES
        }

        val cursor = ByteCursor(source, arraysOffset)
        val replicas = cursor.readCompactIntArray(MAX_REPLICA_COUNT) ?: return null
        val isr = cursor.readCompactIntArray(MAX_REPLICA_COUNT) ?: return null
        cursor.readCompactIntArray(MAX_REPLICA_COUNT) ?: return null // removing replicas
        cursor.readCompactIntArray(MAX_REPLICA_COUNT) ?: return null // adding replicas

        val leaderId = cursor.readInt() ?: return null

        val leaderEpoch = parseLeaderEpoch(cursor) ?: return null
        // partition epoch
        cursor.readInt() ?: return null

        // directories field exists in newer record versions; best-effort skip.
        cursor.tryReadCompactUuidArray()
        // tagged fields; best-effort skip.
        cursor.tryReadUnsignedVarInt()

        if (replicas.isEmpty()) {
            return null
        }
        if (!areValidNodeIds(replicas) || !areValidNodeIds(isr)) {
            return null
        }
        if (leaderId >= 0 && !replicas.contains(leaderId)) {
            return null
        }
        if (leaderId !in MIN_NODE_ID..MAX_NODE_ID && leaderId != NO_LEADER_ID) {
            return null
        }
        if (isr.any { !replicas.contains(it) }) {
            return null
        }
        if (leaderEpoch !in 0..MAX_LEADER_EPOCH) {
            return null
        }

        return ClusterPartitionMetadata(
            partitionIndex = partitionIndex,
            leaderId = leaderId,
            leaderEpoch = leaderEpoch,
            replicaNodes = replicas,
            isrNodes = isr,
        )
    }

    private fun parseLeaderEpoch(cursor: ByteCursor): Int? {
        val withRecoveryPosition = cursor.position
        val recoveryState = cursor.readUnsignedByte()
        if (recoveryState != null && recoveryState in 0..1) {
            val leaderEpoch = cursor.readInt()
            if (leaderEpoch != null) {
                return leaderEpoch
            }
        }

        cursor.position = withRecoveryPosition
        return cursor.readInt()
    }

    private fun findAllOffsets(source: ByteArray, target: ByteArray): List<Int> {
        if (target.isEmpty() || source.size < target.size) {
            return emptyList()
        }

        val offsets = mutableListOf<Int>()
        for (start in 0..source.size - target.size) {
            var matches = true
            for (offset in target.indices) {
                if (source[start + offset] != target[offset]) {
                    matches = false
                    break
                }
            }
            if (matches) {
                offsets.add(start)
            }
        }

        return offsets
    }

    private fun isValidTopicName(topicBytes: ByteArray): Boolean {
        if (topicBytes.isEmpty()) {
            return false
        }

        for (b in topicBytes) {
            val c = b.toInt().toChar()
            val isAlphaNum = c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9'
            val isAllowedSymbol = c == '.' || c == '_' || c == '-'
            if (!isAlphaNum && !isAllowedSymbol) {
                return false
            }
        }

        return true
    }

    private fun toBytes(uuid: UUID): ByteArray {
        val buffer = ByteBuffer.allocate(UUID_SIZE_BYTES)
        buffer.putLong(uuid.mostSignificantBits)
        buffer.putLong(uuid.leastSignificantBits)
        return buffer.array()
    }

    private fun areValidNodeIds(values: List<Int>): Boolean {
        return values.all { it in MIN_NODE_ID..MAX_NODE_ID }
    }

    private fun readUuid(source: ByteArray, start: Int): UUID {
        val buffer = ByteBuffer.wrap(source, start, UUID_SIZE_BYTES)
        return UUID(buffer.long, buffer.long)
    }

    private fun readInt(source: ByteArray, start: Int): Int {
        val buffer = ByteBuffer.wrap(source, start, INT_SIZE_BYTES)
        return buffer.int
    }

    private data class LocatedPartition(
        val partition: ClusterPartitionMetadata,
        val offset: Int,
    )

    private class ByteCursor(
        private val source: ByteArray,
        var position: Int,
    ) {
        fun remaining(): Int = source.size - position

        fun readUnsignedByte(): Int? {
            if (remaining() < 1) {
                return null
            }

            return source[position++].toInt() and 0xFF
        }

        fun readInt(): Int? {
            if (remaining() < INT_SIZE_BYTES) {
                return null
            }

            val value = ByteBuffer.wrap(source, position, INT_SIZE_BYTES).int
            position += INT_SIZE_BYTES
            return value
        }

        fun readUnsignedVarInt(): Int? {
            var value = 0
            var shift = 0

            while (remaining() > 0 && shift <= 28) {
                val current = readUnsignedByte() ?: return null
                value = value or ((current and 0x7F) shl shift)
                if ((current and 0x80) == 0) {
                    return value
                }
                shift += 7
            }

            return null
        }

        fun readCompactIntArray(maxElements: Int): List<Int>? {
            val encodedLength = readUnsignedVarInt() ?: return null
            if (encodedLength == 0) {
                return null
            }

            val length = encodedLength - 1
            if (length < 0 || length > maxElements) {
                return null
            }
            if (remaining() < length * INT_SIZE_BYTES) {
                return null
            }

            val values = ArrayList<Int>(length)
            repeat(length) {
                values.add(readInt() ?: return null)
            }
            return values
        }

        fun tryReadCompactUuidArray() {
            val checkpoint = position
            val encodedLength = readUnsignedVarInt() ?: run {
                position = checkpoint
                return
            }
            if (encodedLength == 0) {
                position = checkpoint
                return
            }

            val length = encodedLength - 1
            if (length < 0) {
                position = checkpoint
                return
            }
            val bytesToSkip = length * UUID_SIZE_BYTES
            if (remaining() < bytesToSkip) {
                position = checkpoint
                return
            }

            position += bytesToSkip
        }

        fun tryReadUnsignedVarInt() {
            val checkpoint = position
            if (readUnsignedVarInt() == null) {
                position = checkpoint
            }
        }
    }

    companion object {
        const val DEFAULT_CLUSTER_METADATA_LOG_PATH =
            "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

        private const val NO_TAGGED_FIELDS = 0
        private const val MAX_TOPIC_NAME_LENGTH = 249
        private const val UUID_SIZE_BYTES = 16
        private const val INT_SIZE_BYTES = 4
        private const val MAX_PARTITION_INDEX = 100_000
        private const val MAX_REPLICA_COUNT = 128
        private const val MIN_NODE_ID = 0
        private const val MAX_NODE_ID = 10_000
        private const val NO_LEADER_ID = -1
        private const val MAX_LEADER_EPOCH = 10_000_000
        private val ZERO_UUID = UUID(0L, 0L)
    }
}
