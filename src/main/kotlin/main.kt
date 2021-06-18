import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.zeromq.SocketType
import org.zeromq.ZContext
import org.zeromq.ZMQ
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.text.SimpleDateFormat


fun main() {
    val pubThread = PubThread()

    pubThread.start()

    val subThread = SubThread()

    subThread.start()

}

class PubThread: Thread() {

    override fun run() {
        val context = ZContext()
        val socket = context.createSocket(SocketType.PUB)
        socket.bind("tcp://localhost:5897")

        val bufferedReader =  BufferedReader(FileReader(File("data.txt")))

        val formatter = SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

        val start = "02/17/2010 09:30:15"

        val startDateTime = formatter.parse(start).time

        runBlocking<Unit> {
            launch {
                var line: String
                var curDateTime = 0L
                while (bufferedReader.readLine().also { line = it } != null) {
                    val data = line.split(",")
                    val date = formatter.parse(data[0] + " " + data[1])
                    if (date.time > startDateTime) {
                        curDateTime = date.time
                        socket.send(line)
                        break
                    }
                }
                while (bufferedReader.readLine().also { line = it } != null) {
                    val data = line.split(",")
                    val date = formatter.parse(data[0] + " " + data[1])
                    val dateTime = date.time
                    delay(dateTime - curDateTime)
                    socket.send(line)
                    curDateTime = dateTime
                }
            }
        }

    }
}

class SubThread: Thread() {

    private fun getMessage(socketSUB: ZMQ.Socket) = flow {
        while (true) {
            val data = socketSUB.recvStr()
            emit(data)
        }
    }

    override fun run() {
        val context = ZContext()
        val socketSUB = context.createSocket(SocketType.SUB)
        socketSUB.connect("tcp://localhost:5897")
        socketSUB.subscribe("")

        runBlocking {
            getMessage(socketSUB).collect { println(it) }
        }
    }
}
