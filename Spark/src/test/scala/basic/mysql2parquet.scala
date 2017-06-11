package basic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/10/26 0026.
  */
object mysql2parquet {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mysql2parquet")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val host="hdfs://jusfoun2016:8020"


    val forecastingDF=sqlContext.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://192.168.15.15:3306/modeltestdata",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"forecasting_xiaofeizhexinxinzhishu_by_month",
      "user"->"root",
      "password"->"5Rb!!@bqC%"
    )).load()
    forecastingDF.show()
    forecastingDF.write.format("parquet").mode("overwrite").save(host+"/model/modeltestdata")

    val outliers_gdpDF=sqlContext.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://192.168.15.15:3306/modeltestdata",
      "driver"->"com.mysql.jdbc.Driver",
      "dbtable"->"outliers_gdp",
      "user"->"root",
      "password"->"5Rb!!@bqC%"
    )).load()
    outliers_gdpDF.show()
    outliers_gdpDF.write.format("parquet").mode("overwrite").save(host+"/model/modeltestdata")
    sc.stop()
  }

}
