package basic

import java.util.Collections

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Sorting

/**
  * Created by hushiwei on 2017/2/15.
  */
object ReadCsv1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("top10").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile("/Users/hushiwei/IdeaProjects/BigData/Spark/src/test/scala/basic/top10.csv")
    val baseRDD = textRDD.map(line => line.split(",")).filter(_ (2).equals("20150501")).map(arr => (arr(0), arr(1).toInt))
    val groupbykeyRDD = baseRDD.groupByKey()
    val resultRDD=groupbykeyRDD.map(tu => {
      val list = tu._2.toList.sorted(Ordering.Int.reverse)
      var arr = ListBuffer[Int]()

      if (list.length < 10) {
        for (i <- 0 until list.length) {
          arr += list(i)
        }
      } else {
        for (i <- 0 to 9) {
          arr += list(i)
        }
      }

      (tu._1, arr.toArray)
    })

    resultRDD.foreach(tu=>println(tu._1+" : "+tu._2.mkString(",")))


  }

}
