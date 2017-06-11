package dataframe

import org.apache.spark.sql.{Column, SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by HuShiwei on 2016/8/15 0015.
  */
object NASALog {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WEB LOG").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logPath="hdfs://ubt202:8020/hsw/access_log_Aug95"
    val sqlContext = new SQLContext(sc)
    val base_df=sqlContext.read.text(logPath)
    base_df.show(truncate=false)

    val split_df=base_df.select(functions.regexp_extract(base_df.col("value"), """^([^\s]+\s)""", 1).alias("host"),
      functions.regexp_extract(base_df.col("value"),"""^.*\\[(\\d\\d/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]""",1).alias("timestamp"),
      functions.regexp_extract(base_df.col("value"),"""^.*"(\w{3,5}).*""",1).alias("method"),
      functions.regexp_extract(base_df.col("value"),"""^.*"\w+\s+([^\s]+)\s+HTTP.*""",1).alias("path"),
      functions.regexp_extract(base_df.col("value"),"""^.*"\s+([^\s]+)""",1).cast("integer").alias("status"),
      functions.regexp_extract(base_df.col("value"),"""^.*\s+(\d+)$""",1).cast("integer").alias("content_size"))
    split_df.show(truncate=false)


    def count_null(col_name:String)=functions.sum(functions.col(col_name)).alias(col_name)

    val exprs=new ArrayBuffer[Column]()
    for (col_name <- split_df.columns) {
      exprs.append(count_null(col_name))
    }

//    注意这个算子，还有把dataframe,Column类型再理解深入一些
//   split_df.agg()


  }

}
