package com.hsw.localapi

import org.apache.spark.sql.SparkSession


/**
  * Created by hushiwei on 2018/1/4.
  * desc : 
  */
object demo01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()



  }

}
