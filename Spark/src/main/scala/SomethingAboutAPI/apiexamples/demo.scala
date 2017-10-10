package SomethingAboutAPI.apiexamples

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by hushiwei on 2017/6/22.
  */
object demo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd=sc.parallelize(1 until 10,2)

    val pairRDD=rdd.map(num=>{
    val ranGen = new Random
      val arr=new Array[String](10)
      for (i<- 0 until arr.length) {
        arr(i)="hello"+ranGen.nextInt(10)
      }
      (num,arr)
    })

pairRDD.foreach{case (x,y)=>{
  val arr=y.mkString(",")
  println(x+":"+arr)
}}
  }
}
