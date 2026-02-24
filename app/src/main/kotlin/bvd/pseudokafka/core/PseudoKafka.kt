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

        repeat(6) { buffer.get() }
        val apiVersion = ByteArray(2).also { buffer.get(it) }
        val rawCorrelationId = ByteArray(4).also { buffer.get(it) }

        val output = if (apiVersion[0] != 0x00.toByte() || apiVersion[1] > 0x04.toByte()) {
            byteArrayOf(0x00, 0x00, 0x00, 0x00)
                .plus(rawCorrelationId)
                .plus(byteArrayOf(0x00, 0x23))
        } else {
            byteArrayOf(0x00, 0x00, 0x00, 0x13)
                .plus(rawCorrelationId)
                .plus(
                    byteArrayOf(
                        0x00,
                        0x00,
                        0x02,
                        0x00,
                        0x12,
                        0x00,
                        0x00,
                        0x00,
                        0x04,
                        0x00,
                        0x00,
                        0x00,
                        0x00,
                        0x00,
                        0x00
                    )
                )
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
}
