package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hushiwei on 2018/1/10.
  * desc : 
  */
object SimpleStreamingApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("First Streaming App").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream=ssc.socketTextStream("localhost",9999)

    stream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
