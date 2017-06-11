package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object UnionTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("union").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data1 = Array[(String, Int)](("K1", 1), ("K2", 2),
      ("U1", 3), ("U2", 4),
      ("W1", 5), ("W1", 6)
    )

    val data2 = Array[(String, Int)](("K1", 7), ("K2", 8),
      ("U1", 9), ("W1", 0)
    )
    val pairs1 = sc.parallelize(data1, 3)
    val pairs2 = sc.parallelize(data2, 2)
    val result = pairs1.union(pairs2)
    result.foreach(println)
  }
}