package SparkStreaming.sparkstreamingBase

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2018/3/6.
  * desc : 
  */
object kafka2sparkstreaming {
  def main(args: Array[String]): Unit = {
    val myTopic = Set("my_test")

    // 设置kafka参数
    val brokers = "10.10.25.13:9092"
    val kafkaParams = Map[String, String]("group.id" -> "heheda",
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest",
      "serializer.class" -> "kafka.serializer.StringEncoder")

    // 设置sparkConf
    val conf = new SparkConf().setAppName("kafka-sparkstreaming-directstream").set("spark.SparkStreaming.streaming.unpersist", "true").set("spark-serializer", "org.apahce.spark.serializer.KvyoSerializer").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 设置streaming上下文
    val ssc = new StreamingContext(sc, Seconds(2))

    //设置检查点
    val checkPoint = "/tmp/sparkstreaimg/tmp_kafka"
    ssc.checkpoint(checkPoint)

    val lineDstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, myTopic)

//    lineDstream.foreachRDD(rdd => rdd.foreach(println))

    var offsetRanges = Array[OffsetRange]()

    val offsetDstream = lineDstream.transform { rdd =>
      offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    offsetDstream.foreachRDD { rdd =>
      rdd.foreach(x => println(x._2))
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
