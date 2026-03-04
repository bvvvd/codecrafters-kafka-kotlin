package bvd.pseudokafka.core

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

    constructor(bufferSize: Int = 1024 * 1024) {
        serverChannel.configureBlocking(false)
        serverChannel.bind(InetSocketAddress("127.0.0.1", 9092))
        serverChannel.register(selector, SelectionKey.OP_ACCEPT)
        buffer = ByteBuffer.allocate(bufferSize)
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

        buffer.int
        val apiKey = buffer.short
        val apiVersion = buffer.short
        val correlationId = buffer.int

        val output = if (apiVersion !in MIN_API_VERSION..MAX_API_VERSION) {
            buildErrorResponse(correlationId)
        } else {
            when (apiKey) {
                API_VERSIONS_KEY -> buildApiVersionsResponse(correlationId)
                else -> buildErrorResponse(correlationId)
            }
        }

        val writeBuf = ByteBuffer.wrap(output)

        var state = key.attachment() as? ClientState
        if (state == null) {
            state = ClientState(key)
            key.attach(state)
        }

        state.pendingWrites.add(writeBuf)

        key.interestOps(key.interestOps() or SelectionKey.OP_WRITE)
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

    private fun buildErrorResponse(correlationId: Int): ByteArray {
        val response = ByteBuffer.allocate(10)
        response.putInt(6)
        response.putInt(correlationId)
        response.putShort(UNSUPPORTED_VERSION_ERROR_CODE)
        return response.array()
    }

    private fun buildApiVersionsResponse(correlationId: Int): ByteArray {
        val supportedApis = listOf(
            ApiVersion(API_VERSIONS_KEY, MIN_API_VERSION, MAX_API_VERSION),
            ApiVersion(DESCRIBE_TOPIC_PARTITIONS_KEY, 0, 0)
        )

        val body = ByteBuffer.allocate(64)
        body.putShort(NO_ERROR_CODE)
        writeUnsignedVarInt(body, supportedApis.size + 1)
        supportedApis.forEach { api ->
            body.putShort(api.apiKey)
            body.putShort(api.minVersion)
            body.putShort(api.maxVersion)
            body.put(NO_TAGGED_FIELDS)
        }
        body.putInt(0)
        body.put(NO_TAGGED_FIELDS)

        val bodyLength = body.position()
        val response = ByteBuffer.allocate(4 + 4 + bodyLength)
        response.putInt(4 + bodyLength)
        response.putInt(correlationId)
        response.put(body.array(), 0, bodyLength)
        return response.array()
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

    private data class ApiVersion(val apiKey: Short, val minVersion: Short, val maxVersion: Short)

    private companion object {
        const val API_VERSIONS_KEY: Short = 18
        const val DESCRIBE_TOPIC_PARTITIONS_KEY: Short = 75
        const val MIN_API_VERSION: Short = 0
        const val MAX_API_VERSION: Short = 4
        const val NO_ERROR_CODE: Short = 0
        const val UNSUPPORTED_VERSION_ERROR_CODE: Short = 35
        const val NO_TAGGED_FIELDS: Byte = 0
    }
}
