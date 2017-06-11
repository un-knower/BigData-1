package SparkSourceCodeLearning

import java.util.concurrent.atomic.AtomicInteger

/**
  * Created by HuShiwei on 2016/9/20 0020.
  */
object AboutJobID {
  def main(args: Array[String]) {
    val nextJobID = new AtomicInteger(10)
    var i = 9
    while (i > 0) {
      val jobID = nextJobID.getAndIncrement()
      println(jobID)
      i = i - 1

    }

  }

}
