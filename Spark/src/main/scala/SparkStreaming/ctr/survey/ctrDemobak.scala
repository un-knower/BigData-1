package SparkStreaming.ctr.survey

import SparkStreaming.params.ConfParms
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by HuShiwei on 2016/8/10 0010.
  */

/**
  */
object ctrDemobak {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  //  定义dsp监播日志的case class
  case class DSPTracker(adx_id: String, campaignId: String, advId: String, event_action: String, day: String, event_time: String)

  /**
    * 初始化spark程序
    */
  def initializeContext(): Any = {
    val sparkConf = new SparkConf().setAppName(ConfParms.APP_NAME)
      .setMaster(ConfParms.MODE)

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Minutes(ConfParms.DURATION))
    (sc, ssc)
  }

  /**
    * 累积求和的transform
    */
  val computeRunningSum = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0L)(_ + _)
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  def main(args: Array[String]) {
    println("start...")

    val (sc: SparkContext, ssc: StreamingContext) = initializeContext

    ssc.checkpoint(ConfParms.CHECKPOINTDIR)

    // Create direct kafka stream with brokers and topics
    val topics = ConfParms.TOPICS.split(",").toSet
    val (broker, groupid) = (ConfParms.BROKERS, ConfParms.GROUPID)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "group.id" -> groupid,
      "serializer.class" -> "kafka.serializer.StringEncoder"
      //"auto.offset.reset" -> config.getString("kafka.offset_reset") // Wrong value latest of auto.offset.reset in ConsumerConfig; Valid values are smallest and largest earliest
    )


    val cleanedDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
      .map { line =>

        println(line)
        val map = mutable.Map[String, String]()
        DSPTrackerParse.parse(map, line)
        val adxid = map.getOrElse("adx_id", "")
        val campaignId = map.getOrElse("campaignId", "")
        val advId = map.getOrElse("advId", "")
        val bidId = map.getOrElse("bidId", "")
        val eventAction = map.getOrElse("eventAction", "")
        val day = map.getOrElse("day", "")
        val time = map.getOrElse("time", "")

        if (advId != "" && time != None && day != None) {
          DSPTracker(adxid, campaignId, advId, eventAction, day, time)
        } else {
          DSPTracker("", "", "", "", "", "")
        }
      }

    printDiv("print impDStream")

    val imphashKey = "adxid:advid:impNum"

    val impPairDstream = cleanedDStream.filter(_.event_action.equalsIgnoreCase("imp")).map(imp => (imp.advId + imp.campaignId, 1L)).reduceByKey(_ + _).cache()
    //    impPairDstream.print()
    //实时求和
    impPairDstream.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(iter => {

          val key = iter._1
          val impCount = iter._2
          println("Real time --->" + key + ":" + impCount)

        })
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

  def printDiv(word: String) = {
    println("~" * 50 + " " + word + " " + "~" * 50)
  }


}
