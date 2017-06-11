package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object JoinAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JoinAction").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data1 = Array[(String, Int)](("A1", 1), ("A2", 2),
      ("B1", 3), ("B2", 4),
      ("C1", 5), ("C1", 6)
    )

    val data2 = Array[(String, Int)](("A1", 7), ("A2", 8),
      ("B1", 9), ("C1", 0)
    )
    val pairs1 = sc.parallelize(data1, 3)
    val pairs2 = sc.parallelize(data2, 2)


    val result = pairs1.join(pairs2)
    result.foreach(println)
  }



}