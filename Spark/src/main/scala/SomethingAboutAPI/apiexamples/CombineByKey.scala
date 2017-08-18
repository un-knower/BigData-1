package SomethingAboutAPI.apiexamples

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("CombineByKey Test").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val a = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val b = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val c = b.zip(a)


    
    val d = c.combineByKey(List(_), (x:List[String], y:String) 
        => y :: x, (x:List[String], y:List[String]) => x ::: y)
        
    val result = d.collect
    result.foreach(println)
    println("RDD graph:\n" + d.toDebugString)
  }
}