package SomethingAboutAPI.apiexamples

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Checkpoint {
  def main(args: Array[String]) {

    var numMappers = 10
    var numPartition = 10
    var numKVPairs = 100
    var valSize = 100
    var numReducers = 3

    val conf = new SparkConf().setAppName("Checkpoint").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/tmp/sparkdata")
    //  造一些测试数据
    //    0到10，10次循环，10个分区
    //    每次循环里面造100个键值对
    val pairs1 = sc.parallelize(0 until numMappers, numPartition).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(10), byteArr)
      }
      arr1
    }.cache
    // cache一下
    //    因此一共是10X100=1000个
    println("pairs1.count: " + pairs1.count)

    val result = pairs1.groupByKey(numReducers)
    result.checkpoint

    //    造数据的时候，key的取值是10以内，所以分组肯定是10个，和结果一样
    println("result.count: " + result.count)
    println(result.toDebugString)

    sc.stop()


  }
}