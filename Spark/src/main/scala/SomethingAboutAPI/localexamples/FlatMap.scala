package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlatMap").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.broadcast()
    val array=Array[String]("Hello","World","hadoop")
    val strRDD=sc.parallelize(array)
    val str2arrayRDD=strRDD.flatMap(x=>x.toCharArray)
    str2arrayRDD.foreach(println)
  }
}