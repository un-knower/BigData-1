package SparkStreaming.sparkstreamingBase

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/8/17 0017.
  */
object demo02 {
  def main(args: Array[String]): Unit = {
    /*  if(args.length != 2){
        println("Usage: <inputpath> <outputpath> ")
        System.exit(1)
      }*/
    //构造spark上下文对象
    val conf = new SparkConf().setAppName("textFileStream")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(20))
    val fileStream: DStream[String] = ssc.textFileStream("E:\\GitHub\\BigData\\logs")
    fileStream.foreachRDD(rdd=>rdd.foreachPartition(iter=>iter.foreach(println)))
    ssc.start()
    ssc.awaitTermination()

  }

  /*  while(true){
      println("死循环中........."+Thread.currentThread())
      Thread.sleep(10000)
    }*/
}
