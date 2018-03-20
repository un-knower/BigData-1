package com.hsw.basicapi

import org.apache.spark.sql.SparkSession

/**
  * Created by hushiwei on 2018/3/20.
  * desc : Logical Plans for Dataframe and Dataset
  */
object DatasetVsDataFrame {

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._


    //read data from text file

    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("Spark22/src/main/resources/sales.csv")
    val ds = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("Spark22/src/main/resources/sales.csv").as[Sales]

    df.show()

    ds.show()

    val selectedDF = df.select("itemId")

    val selectedDS = ds.map(_.itemId)

    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)

    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)


  }
}
