package SomethingML.TF

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2018/1/12.
  * desc : 
  */
object basic {


  def main(args: Array[String]): Unit = {
    case class RawDataRecord(category: String, text: String)
    val conf = new SparkConf().setAppName("TF").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val rawtxt = sc.textFile("Spark/src/main/resources/ml/C000014.txt")




  }

}
