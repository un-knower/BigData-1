package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object reduceActionTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data1 = Array[(String, Int)](("K1", 1), ("K2", 2),
      ("U1", 3), ("U2", 4),
      ("W1", 3), ("W2", 4)
    )
    val pairs = sc.parallelize(data1, 3)
    val result = pairs.reduce((A, B) => (A._1 + "#" + B._1, A._2 + B._2))
    println(result)
  }

}