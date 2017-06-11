package SomethingAboutExamples.userword

import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by HuShiwei on 2016/10/13 0013.
  */
object UserWord extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("userword").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://jusfoun2016:8020/hsw/userword.txt").cache()
    val arrRDD = rdd.map(line => line.split(","))
    val result = new mutable.HashMap[String, ArrayBuffer[mutable.HashMap[String, Int]]]()

    arrRDD.foreach(arr => {
      if (!result.contains(arr(0))) {
        println(arr.mkString("-"))
        val tempList = new ArrayBuffer[mutable.HashMap[String, Int]]()
        var value = 1
        for (map <- tempList) {
          if (!map.contains(arr(1))) {
            val tempMap = new mutable.HashMap[String, Int]()
            if (!tempMap.contains(arr(1))) {
              tempMap += (arr(1) -> value)
            } else {
              value += 1
              tempMap += (arr(1) -> value)
            }
            tempList.append(tempMap)
          } else {
            value += 1
            map += (arr(1) -> value)
          }
        }


      } else {

      }
    })

//    result.foreach(case (x,y)=>)
  }
}
