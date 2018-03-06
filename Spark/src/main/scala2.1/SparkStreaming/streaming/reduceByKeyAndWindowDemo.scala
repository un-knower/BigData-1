package SparkStreaming.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hushiwei on 2017/6/12.
  * 基于滑动窗口的热点搜索词实时统计
  * 每隔5秒钟，统计最近20秒钟的搜索词的搜索频次，
  * 并打印出排名最靠前的3个搜索词以及出现次数
  *  在mac pro 输入 nc -lk 9999
  */
object reduceByKeyAndWindowDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("WindowDemo").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(5))

    //从nc服务中获取数据,数据格式:name word,比如:张三 大数据
    val linesDStream=ssc.socketTextStream("localhost",9999)

    //将数据中的搜索词取出
//    val wordsDStream=linesDStream.flatMap(_.split(" ")(1))


    //通过map算子，将搜索词形成键值对(word,1),将搜索词记录为1次
    val searchwordDStream=linesDStream.map(searchword=>(searchword,1))

    //通过reduceByKeyAndWindow算子,每隔5秒统计最近20秒的搜索词出现的次数
    val reduceDStream=searchwordDStream.reduceByKeyAndWindow(
      (v1:Int,v2:Int)=>
        v1+v2,Seconds(20),Seconds(5)
    )

    //调用DStream中的transform算子,可以进行数据转换
    val transformDStream=reduceDStream.transform(searchwordRDD=>{
      val result=searchwordRDD.map(m=>{  //将key与value互换位置
        (m._2,m._1)
      }).sortByKey(false) //根据key进行降序排列
        .map(m=>{ //将key与value互换位置
        (m._2,m._1)
      }).take(3) //取前3名


      for(elem<-result){
        println(elem._1+"  "+elem._2)
      }
      searchwordRDD //注意返回值
    })

    transformDStream.print()


    ssc.start()
    ssc.awaitTermination()






  }

}
