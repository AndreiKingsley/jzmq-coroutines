import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ

fun main() {
    val pubThread = PubThread()

    pubThread.start()

    val subThread = SubThread()

    subThread.start()

}

class PubThread: Thread() {

    override fun run() {
        val context = ZContext()
        val socketPUB = context.createSocket(SocketType.PUB)
        socketPUB.bind("tcp://localhost:5897")

        runBlocking<Unit> {
            launch {
                for (i in 1..100) {

                    delay(100)

                    val message = "Publisher1 $i"

                    socketPUB.send(message)

                }
            }
        }
    }
}

class SubThread: Thread() {

    private fun getMessage(socketSUB: ZMQ.Socket) = flow {
        while (true) {
            val data = socketSUB.recvStr()
            emit(data.split(" ")[1])
        }
    }

    override fun run() {
        val context = ZContext()
        val socketSUB = context.createSocket(SocketType.SUB)
        socketSUB.connect("tcp://localhost:5897")
        socketSUB.subscribe("Publisher1")

        runBlocking {
            getMessage(socketSUB).collect { println(it) }
        }
    }
}
