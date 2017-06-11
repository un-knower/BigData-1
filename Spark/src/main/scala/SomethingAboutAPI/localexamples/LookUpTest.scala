package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

object LookUpTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LookUp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6))


    val pairs = sc.parallelize(data, 3)

    val finalRDD = pairs.lookup("B")

    finalRDD.foreach(println)
    // output:
    // 2
    // 3


  }
}