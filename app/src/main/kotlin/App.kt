import java.net.ServerSocket

fun main() {
    val serverSocket = ServerSocket(9092)
    // // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // // ensures that we don't run into 'Address already in use' errors
    serverSocket.reuseAddress = true
    serverSocket.accept()
        .use { client ->
            client.getInputStream().use { inputStream ->
                inputStream.skipNBytes(6)
                val apiVersion = inputStream.readNBytes(2)
                val rawCorrelationId = inputStream.readNBytes(4)
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

                client.getOutputStream().use { outputStream -> outputStream.write(output) }

            }
        }
}
