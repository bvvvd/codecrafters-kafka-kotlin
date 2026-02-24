package bvd.pseudokafka.utils

fun debug(msg: String, vararg args: Any) {
    println("[DEBUG] " + msg.format(*args))
}

fun error(msg: String, vararg args: Any) {
    println("[ERROR] " + msg.format(*args))
}