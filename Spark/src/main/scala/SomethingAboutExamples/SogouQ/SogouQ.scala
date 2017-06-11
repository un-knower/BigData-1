package SomethingAboutExamples.SogouQ

import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/6/16 0016.
  */

/**
  * 分析 用户查询日志(SogouQ)
  * 00:00:01	14918066497166943	[欧洲冠军联赛决赛]	4 1	s.sohu.com/20080220/n255256097.shtml
  * 数据格式为
  * 访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
  * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID。
  */
object SogouQ extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SogouQ-Process").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sogouRDD = sc.textFile("hdfs://jusfoun2016:8020/hsw/SogouQ.sample").cache()
    logWarning(s"SogouQ.sample文件中共有${sogouRDD.count()}行")
    //    val taskContext=org.apache.spark.TaskContext.get()
    //    val stageID=taskContext.stageId()
    //    println("stageID ==>"+stageID)

    val persistRDD = sogouRDD.map(line => line.split("\t")).filter(_.length == 5).cache()
    logWarning(s"${persistRDD.count()}")


    /** 20点至24点共有多少行 **/
    val resNum = sogouRDD.map(line => line.split("\t")(0)).filter(x => x >= "08:00:00" && x < "24:00:00").count()
    logWarning(s"20点至24点共有${resNum}行")

    /** 搜索结果排名第一，但是点击次序排在第2的数据有多少 **/
    val resNum2 = persistRDD.map(_ (3).split(' ')).filter(x => x(0).toInt == 1 && x(1).toInt == 2).count()
    logWarning(s"搜索结果排名第一，但是点击次序排在第2的数据有$resNum2 行")

    /** 一个session内查询次数最多的用户的session与相应的查询次数 **/
    persistRDD.map(x => (x(1), 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      //按照Key进行排序的（K，V）对数据集。升序或降序由ascending布尔参数决定,false降序，true升序
      .sortByKey(false)
      .take(10)
      .foreach(println)

    //wordcount来一个
    //    sogouRDD.map(line=>(line.split("\t")(1),1)).reduceByKey(_+_)

    sc.stop()


  }

}
