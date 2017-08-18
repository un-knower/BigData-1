package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2017/6/19.
  * leftOuterJoin类似于SQL中的左外关联left outer join，
  * 返回结果以前面的RDD为主，关联不上的记录为空。
  * 只能用于两个RDD之间的关联，如果要多个RDD关联，多关联几次即可。
  */
object LeftOuterJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LeftOuterJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1=sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    val rdd2=sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),6)

    val joinedRDD=rdd1.leftOuterJoin(rdd2)
    joinedRDD.collect().foreach(println(_))
  }

}
