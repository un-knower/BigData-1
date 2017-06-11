package SomethingAboutExamples.hotel2000W

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.csv._

/**
  * Created by HuShiwei on 2016/11/30 0030.
  */
object hotelRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KaiFan").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    val path="G:\\2000W\\1-200W.csv"
    //    val textFile=sc.textFile(path)
    //    酒店开房数据的字段
    //    Name,CardNo,Descriot,CtfTp,CtfId,Gender,Birthday,Address,Zip,Dirty,District1,District2,District3,District4,District5,District6,FirstNm,LastNm,Duty,Mobile,Tel,Fax,EMail,Nation,Taste,Education,Company,CTel,CAddress,CZip,Family,Version,id
    var hotel1RDD = sc.textFile("G:\\2000W\\1-200W.csv")
    //    var hotel2RDD = sc.textFile("G:\\2000W\\200W-400W.csv")
    //    var hotel3RDD = sc.textFile("G:\\2000W\\400W-600W.csv")
    //    var hotel4RDD = sc.textFile("G:\\2000W\\600W-800W.csv")
    //    var hotel5RDD = sc.textFile("G:\\2000W\\800W-1000W.csv")
    //    var hotel6RDD = sc.textFile("G:\\2000W\\1000W-1200W.csv")
    //    var hotel7RDD = sc.textFile("G:\\2000W\\1200W-1400W.csv")
    //    var hotel8RDD = sc.textFile("G:\\2000W\\1400W-1600W.csv")
    //    var hotel9RDD = sc.textFile("G:\\2000W\\1600w-1800w.csv")
    //    var hotel10RDD = sc.textFile("G:\\2000W\\1800w-2000w.csv")

    //    var hotelRDD = hotel1RDD ++ hotel2RDD ++ hotel3RDD ++ hotel4RDD ++ hotel5RDD ++ hotel6RDD ++ hotel7RDD ++ hotel8RDD ++ hotel9RDD ++ hotel10RDD
    var hotelRDD = hotel1RDD

    //    分析开房次数TOP10用户(第0,3,4三个字段确定唯一的用户)
    //    val tmpRDD=hotelRDD.map(line=>line.toString().split(",")).filter(_.length==33)
    //    val result=tmpRDD.map(arr=> (arr(0),arr(3),arr(4))).map(arr=>(arr,1)).reduceByKey(_+_).sortBy(arr=>arr._2,false)
    //    result.take(10).foreach(println)

    //    分析离店时段分布
    //    统计一天中,各个小时离店的人数
    //    过滤字段不够的脏数据,过滤时间格式不对的脏数据
    //    val hourRDD = hotelRDD.map(line => line.toString().split(",")).filter(_.length == 33).filter(arr => arr(31).trim.length != 0)
    //      .map(arr => arr(31)).map(str => str.split("[- :]")).filter(arr => arr.length == 6)
    //      .map(arr => arr(3).toInt).map(hour => (hour, 1)).reduceByKey(_ + _).map { case (x, y) => (y, x)}
    //    val sortedHourRDD=hourRDD.sortByKey(false, 1)
    //    sortedHourRDD.take(10).foreach(println)
    //
    //    println("--------------------------------")
    //    hourRDD.top(10).foreach(println)


    //    性别分布统计
/*    val sexRDD = hotelRDD.map(line => line.toString().split(",")).filter(_.length == 33).filter(arr => arr(31).trim.length != 0)
      .map(arr => arr(5))
      //      过滤掉脏数据,性别列可能为null,因此需要判断过滤下
      .filter(_.trim.length == 1)
      .filter(sex => (sex == "F" || sex == "M"))
      .map((_, 1))
      //      对性别字段进行统计
      .reduceByKey(_ + _)

    sexRDD.foreach(println)*/

    //    年龄分布统计(从身份证中截取出身年份,然后用今年减去即可)
/*    val ageRDD = hotelRDD.map(line => line.toString().split(",")).filter(_.length == 33)
      .filter(arr => arr(3) == "ID") //只取ID类型的用户
      .filter(arr => arr(4).length == 18) //确保身份证号的位数是对的
      .map(_ (4))
      .filter("^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}([0-9]|X)$".r.pattern.matcher(_).matches())
      .map(num => num.substring(6, 10)) // 取出年份
      .map(2016 - _.toInt)
      .filter(age => (age > 0 && age < 110))
      .map((_, 1))
      .reduceByKey(_ + _)
      .map { case (x, y) => (y, x) }
      .sortByKey(false, 1)

    println(ageRDD.count())
    ageRDD.foreach(println)*/

//    或得email地址
    val emailRDD=hotelRDD.map(line=>line.toString.split(",")).filter(_.length==33)
      .filter(arr=>"^\\w+([\\.-]?\\w+)*@\\w+([\\.-]?\\w+)*(\\.\\w{2,3})+$".r.pattern.matcher(arr(22)).matches())
      .map(arr=>(arr(22),(arr(0),arr(3),arr(4))))

//    emailRDD.foreach(println)
    println(emailRDD.count())

    val csdnRDD=sc.textFile("G:\\600W-CSDN\\csdnuser.txt")
    val csdnEmail=csdnRDD.map(line=>line.split(" # ")).map(arr=>arr.map(_.trim))
      .filter(arr=>"^\\w+([\\.-]?\\w+)*@\\w+([\\.-]?\\w+)*(\\.\\w{2,3})+$".r.pattern.matcher(arr(2)).matches())
      .map(arr=>(arr(2),(arr(0),arr(1))))

    println(csdnEmail.count())

    println(emailRDD.join(csdnEmail).count())



  }

}
