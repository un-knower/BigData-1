package com.hsw.basicapi

import org.apache.spark.sql.SparkSession

/**
  * Created by hushiwei on 2018/3/20.
  * desc : Spark Session example
  */
object SparkSessionExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().
      master("local")
      .appName("spark session example")
      .getOrCreate()

    val df = sparkSession.read.option("header", "true").csv("Spark22/src/main/resources/sales.csv")

    df.show()


  }

}
