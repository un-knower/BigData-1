package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext, SparkException}

/**
  * Created by HuShiwei on 2016/11/18 0018.
  */
object Zip {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("zip").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    构造数据集
    val numberRDD = sc.parallelize(1 to 20, 4)
    val numberRDDCopy = sc.parallelize(1 to 20, 4)
    val letterRDD = sc.parallelize('a' to 'z', 4)

//    val result = numberRDD.zip(letterRDD)
    val result = numberRDD.zip(numberRDDCopy)
    try {
      result foreach {
        case (c, i) => println(i + ":  " + c)
      }
    } catch {
      case iae: IllegalArgumentException =>
        println("Exception caught IllegalArgumentException: " + iae.getMessage)
      case se: SparkException => {
        val t = se.getMessage
        println("Exception caught SparkException: " + se.getMessage)
      }
    }
  }
}
