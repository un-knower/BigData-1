package SomethingAboutAPI.localexamples

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object GroupByAction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupByAction").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = Array[(String, Int)](("A1", 1), ("A2", 2),
      ("B1", 6), ("A2", 4),
      ("B1", 3), ("B1", 5))

    val pairs = sc.parallelize(data, 3)
    pairs.foreach(println)
    val result1 = pairs.groupBy(K => K._1)
    val result2 = pairs.groupBy((K: (String, Int)) => K._1,1)
    val result3 = pairs.groupBy((K: (String, Int)) => K._1, new RangePartitioner(3, pairs))

    println("*"*20)
    result1.foreach(println)
    println("*"*20)
    result2.foreach(println)
    println("*"*20)

    result3.foreach(println)

  }

}