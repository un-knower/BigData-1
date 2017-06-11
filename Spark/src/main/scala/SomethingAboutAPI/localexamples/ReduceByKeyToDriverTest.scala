package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyToDriverTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data1 = Array[(String, Int)](("K", 1), ("U", 2),
      ("U", 3), ("W", 4),
      ("W", 5), ("W", 6)
    )
    val pairs = sc.parallelize(data1, 3)
    val result = pairs.reduceByKey(_ + _)
    result.foreach(println)
  }
}