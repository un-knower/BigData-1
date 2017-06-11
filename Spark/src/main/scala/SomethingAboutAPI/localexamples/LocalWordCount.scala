package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object LocalWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LocalWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val myFile = sc.textFile("/Users/hushiwei/Downloads/randomText-10MB.txt")

    val counts = myFile.map( l => l.split(" ")(0) )
			.map( url => (url, 1) )
			.reduceByKey( _+_ )
			.map{ case(url, count) => (count, url) }
			.sortByKey( ascending=false )
			.map{ case(count, url) => (url, count) }

    val result=counts.repartition(10)
    result.glom().zipWithIndex().collect().foreach{case (arr,index)=>{
      println("partition: "+index+" --- "+arr.mkString(" , "))
    }}
    println("----------------------")
    result.glom().zipWithIndex().foreachPartition(iter=>{
      iter.foreach{case (x,y)=>println("partition: "+y+" --- "+x.mkString(" , "))}
    })

//    result.zipWithIndex().foreachPartition(iter=>{
//      iter.foreach{case (x,y)=>println("partition: "+y+" word: "+x._1+" count: "+x._2)}
//    })



//    val wordAndCount = myFile.flatMap(s => s.split(" "))
//      .map(w => (w, 1))
//
//    val result = wordAndCount.reduceByKey(_ + _)
//    result.foreach(println)

    def printRDD(rdd:RDD[_])={
      val str=rdd.collect().mkString(" , ")
      println(str)
    }
  }

}