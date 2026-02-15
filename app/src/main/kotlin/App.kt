import java.net.ServerSocket
import java.nio.charset.Charset
import java.util.Arrays

fun main() {
    val serverSocket = ServerSocket(9092)
    // // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // // ensures that we don't run into 'Address already in use' errors
    serverSocket.reuseAddress = true
    serverSocket.accept()
        .use { client ->
            client.getInputStream().use { inputStream ->
                inputStream.skipNBytes(8);
                val rawCorrelationId = inputStream.readNBytes(4)

                client.getOutputStream().use { outputStream ->
                    val output = byteArrayOf(0x00, 0x00, 0x00, 0x00).plus(rawCorrelationId)

                    outputStream.write(output)
                }
            }
        }
}
