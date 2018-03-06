package SparkStreaming.ad

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hushiwei on 2017/6/19.
  * 广告计费系统中在线黑名单过滤实战
  * 在线过滤掉黑名单的点击
  */
object OnlineBlackListFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("OnlineBlackListFilter")
    conf.setMaster("local[*]")

    val ssc=new StreamingContext(conf, Seconds(5))

    val BlackList=Array(("hadoop",true),("spark",true))
    val BlackListRDD=ssc.sparkContext.parallelize(BlackList,8)

    val adsClickStream=ssc.socketTextStream("localhost",9999)

    /**
      * 此处模拟的广告点击的每条数据的格式为:time,name
      * 此处map操作的结果是name,(time,name)的格式
      */
    val adsClickStreamFormatted=adsClickStream.map{ads=>(ads.split(",")(1),ads)}

    adsClickStreamFormatted.transform(userClickRDD=>{
      val joinedBlackListRDD=userClickRDD.leftOuterJoin(BlackListRDD)


      val validClicked=joinedBlackListRDD.filter(joinedItem=> {
        if (joinedItem._2._2.getOrElse(false)) {
          false
        }else{
          true
        }
      })

      validClicked.map(validClicked=>{validClicked._2._1})
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }

}
