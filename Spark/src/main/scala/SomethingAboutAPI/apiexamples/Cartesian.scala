package SomethingAboutAPI.apiexamples

import org.apache.spark.{SparkConf, SparkContext}

object Cartesian {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("cartesian").setMaster("local[*]")
    val sc=new SparkContext(conf)

	  val x = sc.parallelize(List(1, 2, 3, 4, 5))
	  val y = sc.parallelize(List(6, 7, 8, 9, 10))

	  println(x ++ y ++ x)
	  val result = x.cartesian(y)
	  //result.collect
	  result.foreach(println)
  }
}