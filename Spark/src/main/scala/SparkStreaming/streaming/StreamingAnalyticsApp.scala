package SparkStreaming.streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hushiwei on 2018/1/10.
  * desc : 
  */
object StreamingAnalyticsApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("First Streaming App").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    // 基于原始文本生成活动流
    val events = stream.map { record =>
      val event = record.split(",")
      (event(0), event(1), event(2))
    }

    // 计算并输出每一个批次的状态.因为每个批次都会生成RDD
    events.foreachRDD { (rdd, time) =>
      val numPurchases = rdd.count()
      val uniqueUsers = rdd.map { case (user, _, _) => user }.distinct().count()
      val totalRevenue = rdd.map { case (_, _, price) => price.toDouble }.sum()
      val productsByPopularity = rdd.map { case (user, product, price) => (product, 1) }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(_._2)

      val mostPopular = productsByPopularity(0)

      val formatter = new SimpleDateFormat()
      val dateStr = formatter.format(new Date(time.milliseconds))
      println(s"== Batch start time : $dateStr ==")
      println("Total purchases: " + numPurchases)
      println("Unique users: " + uniqueUsers)
      println("Total revenue: " + totalRevenue)
      println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))
    }


    ssc.start()
    ssc.awaitTermination()

  }

}
