package SomethingAboutExamples.hotel2000W

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.csv._
/**
  * Created by HuShiwei on 2016/11/30 0030.
  */
object hotel {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("KaiFan").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val path="G:\\2000W\\last5000.csv"
//    Name,CardNo,Descriot,CtfTp,CtfId,Gender,Birthday,Address,Zip,Dirty,District1,District2,District3,District4,District5,District6,FirstNm,LastNm,Duty,Mobile,Tel,Fax,EMail,Nation,Taste,Education,Company,CTel,CAddress,CZip,Family,Version,id
    val df=sqlContext.csvFile(path)
    df.registerTempTable("hotel")
    val df1=sqlContext.sql("select Name from hotel where Gender='M'")
    df1.show()
//    df.show()


  }

}
