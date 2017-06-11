package basic

import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 17-2-9.
  */
object readAvro1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readFileFromFaFq").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // import needed for the .avro method to be added
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.avro")
//      .load("/Users/hushiwei/IdeaProjects/BigData/Spark/src/test/scala/basic/episodes.avro")
      .load("hdfs://ncp162:8020/wzt/flume/log/pagelog/FlumeData.1487231139868.avro")
    df.show
  }

}
