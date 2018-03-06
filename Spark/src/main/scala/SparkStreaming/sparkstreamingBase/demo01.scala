package SparkStreaming.sparkstreamingBase

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by HuShiwei on 2016/8/10 0010.
  */

/**
  * 实时流数据启动后就不会停止,除非手动停止
  *
  * 启动demo01后
  *
  * 在mac pro 输入 nc -lk 9999
  */
object demo01 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-SparkStreaming.streaming-wordcount")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = ssc.socketTextStream("localhost", 9999)

    //把每一行切成单词
    val words = lines.flatMap(_.split(" "))
//
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
//    把Dstream中的前10个元素打印到控制台
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
