package SomethingAboutExamples.hotel2000W

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/12/6 0006.
  */
object CSDNRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CSDN").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val csdnRDD = sc.textFile("/home/hushiwei/sampledata/600W-CSDN/csdnuser.txt")
    /*    println(s"csdn一共泄露了: ${csdnRDD.count()} 个用户数据")
        //     cafaddcjl # KIC43aafdk6! # ccegfdsdcjl@21cn.com
        //    分析最多人使用的TOPn个密码
        val pwRDD = csdnRDD.map(line => line.toString.split(" # "))
          .map(_ (1))
          .map((_, 1)).reduceByKey(_ + _)
          .map { case (x, y) => (y, x) }.sortByKey(false)
          .map { case (x, y) => (y, x) }
        println("最常用的前50个密码: ")
        pwRDD.take(50).foreach(println)*/

        //    统计使用纯数字作为密码的人数
        val numPwRDD = csdnRDD.map(line => line.toString.split(" # "))
          .map(_ (1))
          .filter("\\d+".r.pattern.matcher(_).matches())
          .map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
        println("最常用的前20个纯数字密码: ")
        numPwRDD.take(20).foreach(println)


    //  统计使用纯字母作为密码的人数
/*    val letterRDD = csdnRDD.map(line => line.toString.split(" # "))
      .map(_ (1))
      .filter("[a-zA-Z]+".r.pattern.matcher(_).matches())
      .map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    println("最常用的前20个纯字母密码: ")
    letterRDD.take(20).foreach(println)*/
  }

}
