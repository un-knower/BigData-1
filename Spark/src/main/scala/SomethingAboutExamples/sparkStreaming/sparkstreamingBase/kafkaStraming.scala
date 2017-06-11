package SomethingAboutExamples.sparkStreaming.sparkstreamingBase

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by HuShiwei on 2016/8/10 0010.
  */

/**
  * 2016-08-17 10:47:18,405 [Thread-1] [Creator] [INFO] - 1003	121.11.87.171	b3422c2d-f3fb-4c93-af17-9fd92921eae7	c13c645a-2458-47e6-a3f1-5d9671d99889	10608	{"ugctype":"fav","userId":"40604","item":"13"}	1	1471402038405
  */
object kafkaStraming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark-kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val zkQuorm = "jusfoun2016:2181,ncp12:2181,ncp11:2181"
    val groupid = "group2"
    val lines = KafkaUtils.createStream(ssc, zkQuorm, groupid, Map("flume-kafka" -> 1)).map(_._2)
    lines.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
//        iter.foreach(println)
        while (iter.hasNext) {
          val line=iter.next()
          val arr=line.split("\t")
          println("ip: "+arr(1))

        }
      })
    })

/*    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKey(_ + _)
    //      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()*/

    ssc.start()
    ssc.awaitTermination()
  }

}
