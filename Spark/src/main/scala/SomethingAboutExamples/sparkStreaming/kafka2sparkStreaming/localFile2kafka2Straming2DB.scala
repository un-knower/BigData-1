package SomethingAboutExamples.sparkStreaming.kafka2sparkStreaming

import SomethingAboutExamples.tvdata.domain.TvBean
import SomethingAboutExamples.tvdata.parserHtml.JsoupUtils
import SomethingAboutExamples.tvdata.utils.DfUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/8/10 0010.
  */

object localFile2kafka2Straming2DB {
  def main(args: Array[String]) {
    var conf: SparkConf = null
    if (args.length == 0) {
      conf = new SparkConf().setAppName("Tvdata").setMaster("local[*]")
    } else {
      conf = new SparkConf().setAppName("Tvdata")
    }
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = new SQLContext(sc)
    ssc.checkpoint("checkpoint")
    val zkQuorm = "jusfoun2016:2181,ncp12:2181,ncp11:2181"
    val groupid = "group2"
    val lines = KafkaUtils.createStream(ssc, zkQuorm, groupid, Map("localFileToKafka" -> 1)).map(_._2)
    val cleanRDD = lines.filter(line => line.contains("<A"))
    val data = cleanRDD.foreachRDD(rdd => {
      val filterRDD = rdd.mapPartitions(iter => {
        iter.map(line => {
          val string = JsoupUtils.parser(line)
          val arr = string.split("@")
          val sn = arr(2).trim
          val tvshow = arr(3).trim
          TvBean(arr(0), arr(1), sn, tvshow, arr(4), arr(5), arr(6))
        })
      }
      )
      import sqlContext.implicits._
      val people = filterRDD.toDF()
      //      people.foreach(println(_))
      DfUtils.df2mysql2(people, "tvdata")

    }

    )


    ssc.start()
    ssc.awaitTermination()
  }

}
