package SomethingAboutExamples.SparkDataProcess.DataProcess02

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/6/8 0008.
  */

/**
  * 用 SQL 语句的方式统计男性中身高超过 180cm 的人数。
  * 用 SQL 语句的方式统计女性中身高超过 170cm 的人数。
  * 对人群按照性别分组并统计男女人数。
  * 用类 RDD 转换的方式对 DataFrame 操作来统计并打印身高大于 210cm 的前 50 名男性。
  * 对所有人按身高进行排序并打印前 50 名的信息。
  * 统计男性的平均身高。
  * 统计女性身高的最大值。
  *
  * 1 M 174
  * 2 F 165
  * 3 M 173
  *
  */
object PeopleDataStatistics {
  private val schemaString = "id,gender,height"

  def main(args: Array[String]) {
    val path = "hdfs://ubt202:8020/hsw/sample_people_info.txt"
    val conf = new SparkConf().setAppName("People Info Statistics")
    val sc = new SparkContext(conf)
    val peopleDataRDD = sc.textFile(path, 5)
    val sqlCtx = new SQLContext(sc)
    val schemaArray = schemaString.split(",")
    val schema = StructType(schemaArray.map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = peopleDataRDD.map(_.split(" ")).map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2)))
    val peopleDF = sqlCtx.createDataFrame(rowRDD, schema)
    peopleDF.registerTempTable("people")
    //get the male people whose height is more than 180
    val highterMale180 = sqlCtx.sql("select id,gender,height from people where height >180 and gender='M'")
    highterMale180.take(100).foreach(println)
    println("Men whose height are more than 180: " + highterMale180.count())
    println("<Display #1>")

    //get the female people whose height is more than 170
    val highterFemale170 = sqlCtx.sql("select id,gender,height from people where height >170 and gender='F'")
    highterFemale170.take(100).foreach(println)
    println("Women whose height are more than 170: " + highterFemale170.count())
    println("<Display #2>")

    //Grouped the people by gender and count the number
    peopleDF.groupBy(peopleDF("gender")).count().show()
    println("People Count Grouped By Gender")
    println("<Display #3>")
    peopleDF.filter(peopleDF("gender").equalTo("M")).filter(
      peopleDF("height")>210
    ).show()
    println("Men whose height is more than 210")
    println("<Display #4>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("M")).agg(Map("height" -> "avg")).show()
    println("The Average height for Men")
    println("<Display #6>")
    //
    peopleDF.filter(peopleDF("gender").equalTo("F")).agg("height" -> "max").show()
    println("The Max height for Women:")
    println("<Display #7>")
    //......
    println("All the statistics actions are finished on structured People data.")
  }

}
