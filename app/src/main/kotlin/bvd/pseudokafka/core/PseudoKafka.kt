package bvd.pseudokafka.core

import bvd.pseudokafka.core.KafkaResponse.*
import bvd.pseudokafka.utils.debug
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.*

class PseudoKafka {
    private val selector: Selector = Selector.open()
    private val serverChannel: ServerSocketChannel = ServerSocketChannel.open()
    private val servingClients: MutableSet<SocketChannel> = mutableSetOf()
    private val buffer: ByteBuffer
    private val clusterMetadataLog: ClusterMetadataLog

    constructor(
        bufferSize: Int = 1024 * 1024,
        clusterMetadataLog: ClusterMetadataLog = ClusterMetadataLog()
    ) {
        serverChannel.configureBlocking(false)
        serverChannel.bind(InetSocketAddress("127.0.0.1", 9092))
        serverChannel.register(selector, SelectionKey.OP_ACCEPT)
        buffer = ByteBuffer.allocate(bufferSize)
        this.clusterMetadataLog = clusterMetadataLog
    }

    fun start() {
        while (!Thread.currentThread().isInterrupted) {
            selector.select(10)
            val selectedKeys = selector.selectedKeys()
            handleKeys(selectedKeys)
            selectedKeys.clear()
        }
    }

    fun handleKeys(selectedKeys: Set<SelectionKey>) {
        for (key in selectedKeys) {
            when {
                key.isAcceptable -> {
                    handleAccept(key)
                }

                key.isReadable -> {
                    handleRead(key)
                }

                key.isWritable -> {
                    handleWrite(key)
                }
            }
        }
    }

    fun handleAccept(key: SelectionKey) {
        val server = key.channel() as ServerSocketChannel
        val client = server.accept()
        client.configureBlocking(false)
        val registeredKey = client.register(selector, SelectionKey.OP_READ or SelectionKey.OP_WRITE)
        registeredKey.attach(ClientState(registeredKey))
        servingClients.add(client)
        debug("Accepted connection from %s", client.remoteAddress)
    }

    private fun handleRead(key: SelectionKey) {
        val client = key.channel() as SocketChannel
        buffer.clear()
        val bytesRead: Int = client.read(buffer)
        if (bytesRead == -1) {
            debug("Client %s disconnected", client.getRemoteAddress())
            client.close()
            servingClients.remove(client)
            return
        }
        buffer.flip()

        val read = KafkaRequest.from(buffer)

        val writeBuf = getResponseBuffer(read)

        var state = key.attachment() as? ClientState
        if (state == null) {
            state = ClientState(key)
            key.attach(state)
        }

        state.pendingWrites.add(writeBuf)

        key.interestOps(key.interestOps() or SelectionKey.OP_WRITE)
    }

    private fun getResponseBuffer(read: KafkaRequest): ByteBuffer {
        return process(read).toByteBuffer()
    }

    private fun process(read: KafkaRequest): KafkaResponse {
        if (read.apiVersion !in KafkaResponse.MIN_API_VERSION..KafkaResponse.MAX_API_VERSION) {
            return ErrorResponse(read.correlationId)
        }

        return when (read.apiKey) {
            KafkaResponse.API_VERSIONS_KEY -> ApiVersionsResponse(
                correlationId = read.correlationId,
                apiVersions = listOf(
                    ApiVersion(
                        apiKey = KafkaResponse.API_VERSIONS_KEY,
                        minVersion = KafkaResponse.MIN_API_VERSION,
                        maxVersion = KafkaResponse.MAX_API_VERSION,
                    ),
                    ApiVersion(
                        apiKey = KafkaResponse.DESCRIBE_TOPIC_PARTITIONS_KEY,
                        minVersion = 0,
                        maxVersion = 0,
                    ),
                ),
            )

            KafkaResponse.DESCRIBE_TOPIC_PARTITIONS_KEY -> {
                val requestedTopicNames = if (read.topicNames.isNotEmpty()) {
                    read.topicNames
                } else {
                    listOf(read.topicName ?: "")
                }
                val metadata = clusterMetadataLog.readMetadata(requestedTopicNames)
                val sortedTopicNames = requestedTopicNames.sorted()

                DescribeTopicPartitionsResponse(
                    correlationId = read.correlationId,
                    topicPartitions = sortedTopicNames.map { topicName ->
                        val topicMetadata = metadata.topic(topicName)
                        val topicId = topicMetadata?.topicId
                        DescribeTopicPartitionsResponse.TopicPartition(
                            topicName = topicName,
                            errorCode = if (topicId == null) {
                                KafkaResponse.UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE
                            } else {
                                KafkaResponse.NO_ERROR_CODE
                            },
                            topicId = topicId ?: KafkaResponse.ZERO_UUID,
                            partitions = topicMetadata
                                ?.partitions
                                ?.map { partition ->
                                    DescribeTopicPartitionsResponse.PartitionMetadata(
                                        partitionIndex = partition.partitionIndex,
                                        leaderId = partition.leaderId,
                                        leaderEpoch = partition.leaderEpoch,
                                        replicaNodes = partition.replicaNodes,
                                        isrNodes = partition.isrNodes,
                                    )
                                }
                                ?: emptyList(),
                        )
                    }
                )
            }

            else -> ErrorResponse(read.correlationId)
        }
    }

    private fun handleWrite(key: SelectionKey) {
        val client = key.channel() as SocketChannel
        val state = key.attachment() as ClientState

        while (!state.pendingWrites.isEmpty()) {
            val stateBuffer = state.pendingWrites.poll()
            if (stateBuffer != null && stateBuffer.hasRemaining()) {
                debug(
                    "sending #%s# to the client: %s",
                    String(stateBuffer.array()),
                    (client.remoteAddress as InetSocketAddress).port
                )
                client.write(stateBuffer)
                if (stateBuffer.hasRemaining()) {
                    return
                }
            }
        }
    }

    private data class ClientState(val key: SelectionKey, val pendingWrites: Queue<ByteBuffer> = ArrayDeque())
}
