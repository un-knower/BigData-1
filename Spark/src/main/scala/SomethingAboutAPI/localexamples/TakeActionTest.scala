package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object TakeActionTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("take").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data1 = Array[(String, Int)](("K1", 1), ("K2", 2),
      ("U1", 3), ("U2", 4),
      ("W1", 3), ("W2", 4)
    )
    val pairs = sc.parallelize(data1, 3)
    val result = pairs.take(5)
    result.foreach(println)
  }
}