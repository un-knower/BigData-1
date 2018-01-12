package streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source
import scala.util.Random

/**
  * Created by hushiwei on 2018/1/10.
  * desc : 
  */
object StreamingProducer {
  def main(args: Array[String]): Unit = {
    val random = new Random()

    // 每秒最大活动数
    val MaxEvents = 6

    // 读取可能的名称
    val namesResource = this.getClass.getResourceAsStream("/names.csv")
    val names = Source.fromInputStream(namesResource)
      .getLines()
      .toList
      .head
      .split(",")
      .toSeq

    // 生成一系列的产品
    val products = Seq(
      "iphone Cover" -> 9.99,
      "Headphones" -> 5.49,
      "Samsung Galaxy Cover" -> 8.95,
      "ipad Cover" -> 7.49
    )


    /**
      * 生成随机产品活动
      *
      * @param n
      * @return
      */
    def generateProductEvents(n: Int) = {
      (1 to n).map { i =>
        val (product, price) =
          products(random.nextInt(products.size))
        val user = random.shuffle(names).head
        (user, product, price)
      }
    }

    // 创建网络生成器
    val listener = new ServerSocket(9999)
    println("Linstening on port : 9999")

    while (true) {

      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from : " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)

          while (true) {
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)
            val productEvents = generateProductEvents(num)
            productEvents.foreach { event =>
              out.write(event.productIterator.mkString(","))
              out.write("\n")
            }
            out.flush()
            println(s"Created $num events...")
          }
          socket.close()
        }
      }.start()
    }


  }

}
