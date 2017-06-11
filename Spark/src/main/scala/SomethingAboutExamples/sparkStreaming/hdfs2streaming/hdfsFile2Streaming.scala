package SomethingAboutExamples.sparkStreaming.hdfs2streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/11/29 0029.
  * 从hdfs读取文件,做实时处理.
  * MakeData.py脚本生成数据
  * makelog.sh 脚本调用MakeData.py 上传数据到hdfs
  */
object hdfsFile2Streaming {
  def main(args: Array[String]) {
    var conf: SparkConf = null
    if (args.length == 0) {
      conf = new SparkConf().setAppName("hdfs log").setMaster("local[*]")
    } else {
      conf = new SparkConf().setAppName("hdfs log")
    }
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val lines=ssc.textFileStream("hdfs://ncp162:8020/hsw/streaming/")
    lines.count().print()

//    lines.repartition()
    ssc.start()
    ssc.awaitTermination()

  }

}
