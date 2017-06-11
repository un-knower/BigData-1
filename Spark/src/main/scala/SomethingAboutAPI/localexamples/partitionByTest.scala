package SomethingAboutAPI.localexamples

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object partitionByTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data1 = Array[(String, Int)](("K", 1), ("T", 2),
      ("T", 3), ("W", 4),
      ("W", 5), ("W", 6)
    )
    val pairs = sc.parallelize(data1, 3)
    val result = pairs.partitionBy(new RangePartitioner(2, pairs, true))
    //val result = pairs.partitionBy(new HashPartitioner(2))
    result.foreach(println)
  }
}