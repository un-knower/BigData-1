package basic

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2017/3/10.
  */
object text2mysql2 {

  case class channel(channel_id: String, channel: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mysql2parquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val textRDD = sc.textFile("/Users/hushiwei/Downloads/channel.txt")
    val rdd = textRDD.map(line => line.split(" -> ")).map(arr => channel(arr(0), arr(1)))
    import sqlContext.implicits._

    val df = rdd.toDF()
    df.show()
    df.registerTempTable("area")


    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "hushiwei")

    df.write.mode("overwrite").jdbc(
      "jdbc:mysql://localhost:3306/wanka?useUnicode=true&characterEncoding=utf-8",
      "company_channel",
      prop
    )
    //    re.rdd.saveAsTextFile("/Users/hushiwei/Downloads/area.propertiessss")

    sc.stop()
  }
}
