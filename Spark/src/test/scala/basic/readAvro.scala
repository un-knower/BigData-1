package basic

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.avro.Schema
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 17-2-9.
  */
object readAvro {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readFileFromFaFq").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // import needed for the .avro method to be added
    import com.databricks.spark.avro._
    val schema = new Schema.Parser().parse(new File("Kafka/avsc/test_schema.avsc"))
    val sqlContext = new SQLContext(sc)
    //
    val df = sqlContext.read.format("com.databricks.spark.avro")
      .option("avroSchema", schema.toString)
      .load("hdfs://ncp162:8020/hsw/flume/log/17-02-09/1536/logs-.1486625773590")
    df.show
  }

}
