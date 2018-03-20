package com.hsw.basicapi

import org.apache.spark.sql.SparkSession

/**
  * Created by hushiwei on 2018/3/20.
  * desc : 
  */
object CatalogExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("catalog example")
      .getOrCreate()

    val df = sparkSession.read.csv("Spark22/src/main/resources/sales.csv")
    df.show()
    df.createTempView("sales")

    // interacting with catalog
    val catalog = sparkSession.catalog

    // print the databases
    catalog.listDatabases().select("name").show()

    // print all the tables
    catalog.listTables().select("name").show()

    // is cached
    println(catalog.isCached("sales"))
    df.cache()
    println(catalog.isCached("sales"))


    // drop the table
    catalog.dropTempView("sales")
    catalog.listTables().select("name").show()

    // list functions
    catalog.listFunctions().select("name", "description", "className", "isTemporary").show(100)

  }
}
