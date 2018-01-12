package SomethingML.Naivebayes

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2018/1/12.
  * desc : 
  */
object demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NaiveBayesExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val df=sqlContext.read.parquet("/Users/hushiwei/demo/nb/nb.txt/data/part-r-00000-80571dc0-c407-48d9-90b0-baf5ac800601.gz.parquet")
    df.show(truncate = false)

  }

}
