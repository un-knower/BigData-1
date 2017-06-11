package basic

import java.io.File

import org.apache.avro.Schema
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by hushiwei on 2017/2/15.
  */
object ReadCsv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("top10").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val customSchema = StructType(Array(
      StructField("userid", StringType, true),
      StructField("pageId", IntegerType, true),
      StructField("date", StringType, true)))

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(customSchema)
      .load("/Users/hushiwei/IdeaProjects/BigData/Spark/src/test/scala/basic/top10.csv")
//求20150501当天每个用户访问页面次数前10的页面。

//    df.show()
    df.registerTempTable("pageclick")
    sqlContext.sql("select userid,pageId from pageclick where date='20150501' ").show(10000)
  }

}
