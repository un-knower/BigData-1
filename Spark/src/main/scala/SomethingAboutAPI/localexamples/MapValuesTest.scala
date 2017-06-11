package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object MapValuesTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("mapValues").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data1 = Array[(String, Int)](("K", 1), ("T", 2),
      ("T", 3), ("W", 4),
      ("W", 5), ("W", 6)
    )
    val pairs = sc.parallelize(data1, 3)
    val result = pairs.mapValues(V => 10 * V)
//    val result=pairs.map { case (k, v) => (k, v * 10) }
    result.foreach(println)
  }
}