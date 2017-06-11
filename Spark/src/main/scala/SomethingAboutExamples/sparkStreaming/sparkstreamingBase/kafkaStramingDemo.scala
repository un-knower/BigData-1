package SomethingAboutExamples.sparkStreaming.sparkstreamingBase

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by HuShiwei on 2016/8/10 0010.
  */

/**
  */
object kafkaStramingDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark-kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val zkQuorm = "ncp161:2181,ncp163:2181,ncp163:2181"
    val groupid = "group2"
    val lines = KafkaUtils.createStream(ssc, zkQuorm, groupid, Map("aaa" -> 1)).map(_._2)
    lines.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
//        iter.foreach(println)
        while (iter.hasNext) {
          val line=iter.next()
         println(line)

        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
