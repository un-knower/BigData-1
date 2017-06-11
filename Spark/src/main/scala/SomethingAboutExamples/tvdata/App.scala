package SomethingAboutExamples.tvdata

import SomethingAboutExamples.tvdata.domain.TvBean
import SomethingAboutExamples.tvdata.parserHtml.JsoupUtils
import SomethingAboutExamples.tvdata.parserXml.parserXmlJava
import SomethingAboutExamples.tvdata.utils.DfUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by HuShiwei on 2016/9/8 0008.
  */
object App {
  private val logger = LoggerFactory.getLogger(App.getClass)

  def main(args: Array[String]) {
    var conf: SparkConf = null
    var path: String = null
    if (args.length == 0) {
      conf = new SparkConf().setAppName("Tvdata").setMaster("local[*]")
      path = "hdfs://jusfoun2016:8020/hsw/tvdata/2012-09-20/ars10768@20120921001500.txt"
    } else {
      conf = new SparkConf().setAppName("Tvdata")
      path = args(0)
    }
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val tvRDD = sc.textFile(path)
    logger.warn(s"===>原始数据有 ${tvRDD.count()} 条")

    val filterRDD = tvRDD.filter(line => line.contains("<A"))
    logger.warn(s"===>过滤之后数据有 ${filterRDD.count()} 条")

    val data = filterRDD.map(line => {
      val string = JsoupUtils.parser(line)
      val arr = string.split("@")
      //      val sn=URLEncoder.encode(arr(2).trim,"utf-8")
      val sn = arr(2).trim
      //      val show=URLEncoder.encode(arr(3).trim,"utf-8")
      val tvshow = arr(3).trim
      TvBean(arr(0), arr(1), sn, tvshow, arr(4), arr(5), arr(6))
    })
//    data.take(1000).foreach(bean => println(bean.toString))
    import sqlContext.implicits._
    val people = data.toDF()
    people.show()
//    people.printSchema()
//    DfUtils.df2mysql2(people, "tvdata")
    //    people.registerTempTable("peopleDemo")
    //    val df = sqlContext.sql("select stbNum,date from peopleDemo")
    //    df.show()
    //    DfUtils.df2mysql(df, "peopleDemo")

    //    val df1=people.select("stbNum","date")
    //    df1.show()
    //    DfUtils.df2mysql(df1, "peopleDemo")


    sc.stop()

  }

}
