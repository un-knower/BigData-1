package SparkStreaming.sparkstreamingBase

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2018/3/6.
  * desc :
  *
  * --conf spark.streaming.kafka.maxRetries=50 \
  * --conf spark.yarn.maxAppAttempts=10 \
  * --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
  */
object ManagerOffset {

  /**
    * 初始化spark程序
    */
  def initializeContext(): Any = {
    val sparkConf = new SparkConf().setAppName("sparkstreaming-kafka-offset-zk")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    (sc, ssc)
  }

  def processRDD(rdd: RDD[(String, String, String)]): Unit = {
    val message = rdd.map(_._2)
    message.foreach(println)

  }

  def main(args: Array[String]): Unit = {

    val (sc: SparkContext, ssc: StreamingContext) = initializeContext

    // Create direct kafka stream with brokers and topics
    val topics = Set("my_test")
    val (broker, groupid) = ("10.10.25.13:9092", "m_offset")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "group.id" -> groupid,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "largest" // Wrong value latest of auto.offset.reset in ConsumerConfig; Valid values are smallest and largest earliest
    )

    val km = new KafkaManager(kafkaParams)
    val stream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    var offsetRanges = Array[OffsetRange]()

    val message = stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    message.foreachRDD { rdd =>
      processRDD(rdd)
      for (o <- offsetRanges) {
        println(s"${o.topic} .... partition->${o.partition} .... fromOffset->${o.fromOffset} .... untilOffset->${o.untilOffset}")
      }
      km.updateZKOffsets(rdd)
    }


    ssc.start()
    ssc.awaitTermination()

  }

}
