package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.mutable

object CollectAsMap {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CollectAsMap").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6))

    // as same as "val pairs = sc.parallelize(data, 3)" 底层源码,也还是调用的parallelize
    val pairs = sc.makeRDD(data, 3)
    val result = pairs.collectAsMap

    // output Map(A -> 1, C -> 6, B -> 3)
    print(result)

//    list2map(data)
  }

  def list2map(list: Array[(String, Int)]): mutable.HashMap[String, Int] = {
    val map = new mutable.HashMap[String, Int]
    list.foreach(value=> map.put(value._1,value._2))
    map
  }

}