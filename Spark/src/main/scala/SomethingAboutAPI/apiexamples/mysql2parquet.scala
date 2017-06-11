package SomethingAboutAPI.apiexamples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by hushiwei on 16-12-20.
  */
object mysql2parquet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("jdbc")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://localhost:3306/ouye?characterEncoding=utf8",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> args(0),
      "user" -> "root",
      "password" -> "jusfoun@123"
    )).load()

    dataFrame.show()
    dataFrame.write.parquet("/ouye/"+args(1))
    sc.stop()
  }

}
