package SomethingAboutExamples.SparkDataProcess.DataProcess01

import java.io.{File, FileWriter}

import scala.util.Random

/**
  * Created by HuShiwei on 2016/6/8 0008.
  */
/**
  * 生成 1000 万人口年龄数据的文件
  * 1 34
  * 2 45
  * 3 67
  * 4 78
  * 5 89
  * ....
  */
object SampleDataFileGenerator {
  def main(args: Array[String]) {
    val writer = new FileWriter(new File("E:\\bigdataData\\sample_age_data.txt"), false)
    val rand = new Random()
    val start=System.currentTimeMillis()
    println("SampleDataGenerator is start: "+start)
    for (i <- 1 to 1000000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
    val end=System.currentTimeMillis()
    val time=end-start
    println("SampleDataGenerator is over ! time is "+time)
  }

}
