import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

/**
  * 模拟产生socket数据
  * scalac streamingSimulation.scala
  * scala streamingSimulation /Users/hushiwei/IdeaProjects/BigData/Spark/src/main/scala/SomethingAboutExamples/sparkStreaming/windows/Information 9999 1000
  */

object streamingSimulation {
  def index(n: Int) = scala.util.Random.nextInt(n)

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }

    val filename = args(0)
    println(filename)
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    val listener = new ServerSocket(args(1).toInt)

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(2).toLong)
            val content = lines(index(filerow))
            println("-------------------------------------------")
            println(s"Time: ${System.currentTimeMillis()}")
            println("-------------------------------------------")
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }

}

