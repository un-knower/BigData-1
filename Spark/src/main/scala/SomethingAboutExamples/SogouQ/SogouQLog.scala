package SomethingAboutExamples.SogouQ

import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/9/7 0007.
  */
object SogouQLog extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SogouLogProcess")
    val sc = new SparkContext(conf)

    val sogouRDD = sc.textFile(args(0),10).cache()
    logWarning(s"===> Sogou Log 的原始日志一共有 ${sogouRDD.count()} 条")

    //    日志转成样例类
    val logParser = new SogouAccessLogParser()
    val persistRDD = sogouRDD.map(line => logParser.parseRecord(line)).cache()

    val filterRDD = persistRDD.filter(line => line != None)
    logWarning(s"过滤后只剩4个字段的数据一共还剩 ${filterRDD.count()} 条")

    /** 搜索结果排名第一，但是点击次序排在第2的数据有多少 **/
//    def rank(line: Option[SogouAccessLogRecord]) = line.get.rank.toInt
//    def order(line: Option[SogouAccessLogRecord]) = line.get.rank.toInt
//    val resNum=filterRDD.filter(line=>rank(line)==1 && order(line)==2).count()
//    logWarning(s"搜索结果排名第一，但是点击次序排在第2的数据有$resNum 行")

    /** 一个session内查询次数最多的用户的session与相应的查询次数 **/
    def userID(line:Option[SogouAccessLogRecord])=line.get.userID
    val pairRDD=filterRDD.map(line=>(userID(line),1))
    val start=getCurrentTime
    val sortedRDD=pairRDD
      .reduceByKey(_+_)
        .map(x=>(x._2,x._1))
      .sortByKey(false)
        .map(x=>(x._2,x._1))
    sortedRDD.take(10).foreach(println)
    logWarning(s"reducebykey and sort and print 10 spend time ${getCurrentTime()-start}")

    val start1=getCurrentTime()
    val groupedRDD=pairRDD.groupByKey().map(x=>(x._1,x._2.sum)).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    groupedRDD.take(10).foreach(println)
    logWarning(s"groupbykey and sort and print 10 spend time ${getCurrentTime()-start1}")

    sc.stop()


  }

  def getCurrentTime()= {
    val currentTimes=System.currentTimeMillis()
    logWarning("====>"+currentTimes)
    currentTimes
  }

}
