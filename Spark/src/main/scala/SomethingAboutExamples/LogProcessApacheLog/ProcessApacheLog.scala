package SomethingAboutExamples.LogProcessApacheLog

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2016/5/19.
  */

/**
  * 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  * 64.242.88.10 - - [07/Mar/2004:16:06:51 -0800] "GET /twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1.3&rev2=1.2 HTTP/1.1" 200 4523
  * 64.242.88.10 - - [07/Mar/2004:16:10:02 -0800] "GET /mailman/listinfo/hsdivision HTTP/1.1" 200 6291
  * 64.242.88.10 - - [07/Mar/2004:16:11:58 -0800] "GET /twiki/bin/view/TWiki/WikiSyntax HTTP/1.1" 200 7352
  * 64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 5253
  * 64.242.88.10 - - [07/Mar/2004:16:23:12 -0800] "GET /twiki/bin/oops/TWiki/AppendixFileSystem?template=oopsmore&param1=1.12&param2=1.12 HTTP/1.1" 200 11382
  */
object ProcessApacheLog {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("spark-load-log")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val log = sc.textFile("hdfs://jusfoun2016/hsw/access_log",5)
    log.take(10).foreach(println)

    println("all count "+log.count())
    val logParser=new AccessLogParser


    def getStatusCode(line:Option[AccessLogRecord])={
      line match {
        case Some(l) => l.httpStatusCode
        case None => "0"
      }
    }

//    获取状态码的个数
    val statusNum = log.filter(line => getStatusCode(logParser.parseRecord(line)) == "404").count
    println("statusNum "+statusNum)

//    finding broken URLS
    def getRequest(rawAccessLogString:String):Option[String]={
      val accessLogRecordOption= logParser.parseRecord(rawAccessLogString)
      accessLogRecordOption match {
        case Some(rec) => Some(rec.request)
        case None => None
      }
    }

    val distinctResc= log.filter(line => getStatusCode(logParser.parseRecord(line)) == "404").map(getRequest(_)).distinct()
    distinctResc.foreach(println)

    def extractUriFromRequest(requestField:String)=requestField.split(" ")(0)

    val distinctRequ=log.filter(line => getStatusCode(logParser.parseRecord(line))=="404")
      .map(getRequest(_))
        .collect{ case Some(requestField) => requestField}
          .map(extractUriFromRequest(_))
            .distinct()

    println(distinctRequ.count())
    distinctRequ.foreach(println)

    sc.stop()

  }
}
