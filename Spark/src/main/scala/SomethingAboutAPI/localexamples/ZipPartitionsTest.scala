package SomethingAboutAPI.localexamples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

/**
  * Created by HuShiwei on 2016/11/18 0018.
  */
object ZipPartitionsTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zipPartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    构造数据集
    val numberRDD = sc.parallelize(1 to 20, 4)
    val letterRDD = sc.parallelize('a' to 'z', 4)
//    printRDD(numberRDD)
//    printRDD(letterRDD)
//    假设所有的partiton里面的元素个数是一样，当不一样的时候，我们需要处理一下咯


    def func(numIter: Iterator[Int], lettIter: Iterator[Char]): Iterator[(Int, Char)] = {
      val arr = new ListBuffer[(Int, Char)]
      while (numIter.hasNext || lettIter.hasNext) {

        if (numIter.hasNext && lettIter.hasNext) {
          arr += ((numIter.next(), lettIter.next()))
        } else if (numIter.hasNext) {
          arr += ((numIter.next(), ' '))
        } else if (lettIter.hasNext) {
          arr += ((0, lettIter.next()))
        }
      }
      arr.iterator
    }

    val result=numberRDD.zipPartitions(letterRDD)(func)
    result.foreach{
      case (k,v)=>println(k+" "+v)
    }
  }

  def printRDD(rdd: RDD[_]) = {
    val nrdd = rdd.glom().zipWithIndex()
    nrdd.foreach {
      case (k, v) =>
        println("第" + v + "个分区数据: " + k.foldLeft("")((x, y) => x + " " + y))
    }
  }

}
