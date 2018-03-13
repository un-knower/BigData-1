package com.hsw.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by hushiwei on 2018/3/8.
  * desc : 
  */
object basic01 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val jsonData=sparkSession.read.json("Spark22/src/main/resources/people.json")






  }

}
