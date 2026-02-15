import java.net.ServerSocket

fun main() {
     val serverSocket = ServerSocket(9092)
    // // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // // ensures that we don't run into 'Address already in use' errors
     serverSocket.reuseAddress = true
    serverSocket.accept()
        .use { client ->
            client.getOutputStream().use {
                it.write(byteArrayOf(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07))
            }
        }
}
