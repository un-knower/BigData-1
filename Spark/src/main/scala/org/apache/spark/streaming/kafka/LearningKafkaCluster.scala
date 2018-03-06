package org.apache.spark.streaming.kafka

/**
  * Created by hushiwei on 2018/3/6.
  * desc : 
  */
object LearningKafkaCluster {
  def main(args: Array[String]): Unit = {

    val topics = Set("my_test")
    val kafkaParams = Map[String, String]("group.id" -> "m_offset",
      "metadata.broker.list" -> "10.10.25.13:9092",
      "auto.offset.reset" -> "largest",
      "serializer.class" -> "kafka.serializer.StringEncoder")

    val kc = new KafkaCluster(kafkaParams)

    val groupId = kafkaParams.get("group.id").get
    val partitionsE=kc.getPartitions(topics)
    val partitions=partitionsE.right.get
    partitions.foreach(println)
    println("-----------------------------")
    val consumerOffsetE=kc.getConsumerOffsets(groupId,partitions)
    consumerOffsetE.right.get.foreach(println)




  }

}
