package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {
  
  def main(args: Array[String]) {

    val conf=new SparkConf().setAppName("Aggregate").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val array=Array[(String,Int)](("Hello",100),("World",100),("Spark",1000),("Storm",500),
      ("Flume",200),("MR",400),("Hive",100)
    )
    val pairs=sc.parallelize(array,2)
    val result=pairs.aggregate(("",0))((U,T)=>(U._1+T._1,U._2+T._2),(U,T)=>
      (U._1+T._1,U._2+T._2))
    println(result)


  }
}