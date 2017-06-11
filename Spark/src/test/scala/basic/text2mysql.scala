package basic

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by hushiwei on 2017/3/10.
  */
object text2mysql {

  case class area( name: String, id: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mysql2parquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val textRDD=sc.textFile("/Users/hushiwei/Downloads/area.properties")
    val rdd=textRDD.map(line=>line.split("=")).map(arr=>area(arr(0),arr(1).toInt))
    import sqlContext.implicits._

    val df=rdd.toDF()
    df.show()
    df.registerTempTable("area")




    val forecastingDF=sqlContext.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://localhost:3306/BigData",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"tb_dmp_group_city",
//      "dbtable"->"tb_dmp_group",
      "user"->"root",
      "password"->"hushiwei"
    )).load()
    forecastingDF.show()
    forecastingDF.registerTempTable("click")

    sqlContext.sql("select a.name ,b.result,b.province from area a right join click b on a.id =b.province").show(100000000)






//        val prop = new Properties()
//        prop.put("driver", "com.mysql.jdbc.Driver")
//        prop.put("user", "root")
//        prop.put("password", "hushiwei")
//
//        re.write.mode("overwrite").jdbc(
//          "jdbc:mysql://localhost:3306/BigData",
//          "area",
//          prop
//        )
//    re.rdd.saveAsTextFile("/Users/hushiwei/Downloads/area.propertiessss")

    sc.stop()
  }
}
