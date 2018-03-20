package com.hsw.basicapi

import org.apache.spark.sql.SparkSession

/**
  * Created by hushiwei on 2018/3/20.
  * desc : 
  */
object DataSetWordCount {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("Spark22/src/main/resources/data.txt").as[String]

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()


  }
}
