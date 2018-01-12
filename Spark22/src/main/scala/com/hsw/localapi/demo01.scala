package com.hsw.localapi

import org.apache.spark.sql.SparkSession


/**
  * Created by hushiwei on 2018/1/4.
  * desc : 
  */
object demo01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val df=spark.read.json("")
    df.show()
  }

}
