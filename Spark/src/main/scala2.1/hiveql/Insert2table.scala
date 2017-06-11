package hiveql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by HuShiwei on 2016/8/17 0017.
  */
object Insert2table {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("insert into hive table").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val hiveContext=new HiveContext(sc)
    import hiveContext.implicits._
    val peopleDF=sc.parallelize(Seq(
      (1,"Tom",23,"NewYork"),
      (2,"Jenny",25,"shanghai"),
      (3,"zs",22,"Hongkong")
    )).toDF("id","name","age","address")

    peopleDF.show()

    peopleDF.write.mode("append").insertInto("hivespark.t1")
    peopleDF.write.mode("overwrite").saveAsTable("hivespark.t2")
//    sc.stop()
    val arr=new ArrayBuffer[String]()
    peopleDF.rdd.foreach(row=>{
      val line=row.mkString(",")
      arr.append(line)
      println("---> "+line)
    })
    println("----------------------")
/*    for (a<- arr) {
      println("===> "+a)
    }*/

    arr foreach {case a => println("===> "+a)}
    hiveContext.sql("use hivespark")
    hiveContext.read.table("t1").show()


  }

}
