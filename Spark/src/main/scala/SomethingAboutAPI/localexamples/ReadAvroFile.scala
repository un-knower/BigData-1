package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object ReadAvroFile {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("avro").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val path = "hdfs://ncp162:8020/hsw/flume/log/17-02-10/1816/logs-.1486721760238"
    val rdd=sc.textFile(path)
    val d=rdd.take(2)
    d.foreach(println)
//    rdd.foreach()
  }
}