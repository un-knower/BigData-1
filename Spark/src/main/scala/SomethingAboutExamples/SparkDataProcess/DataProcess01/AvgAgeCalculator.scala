package SomethingAboutExamples.SparkDataProcess.DataProcess01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/6/8 0008.
  */

/**
  * 要计算平均年龄，那么首先需要对源文件对应的 RDD 进行处理，也就是将它转化成一个只包含年龄信息的 RDD，其次是计算元素个数即为总人数，然后是把所有年龄数加起来，最后平均年龄=总年龄/人数。
  * 对于第一步我们需要使用 map 算子把源文件对应的 RDD 映射成一个新的只包含年龄数据的 RDD，很显然需要对在 map 算子的传入函数中使用 split 方法，得到数组后只取第二个元素即为年龄信息；第二步计算数据元素总数需要对于第一步映射的结果 RDD 使用 count 算子；第三步则是使用 reduce 算子对只包含年龄信息的 RDD 的所有元素用加法求和；最后使用除法计算平均年龄即可。
  * 由于本例输出结果很简单，所以只打印在控制台即可。
  */
object AvgAgeCalculator {
  def main(args: Array[String]) {
/*    if (args.length < 1) {
      println("Usage:AvgAgeCalcutor datafile")
      System.exit(1)
    }*/
    val path="hdfs://ubt202:8020/hsw/sample_age_data.txt"
    val conf = new SparkConf().setAppName("Spark Exercise:Average Age Calculator").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile(path, 5)
    val count = dataFile.count()
    val ageData = dataFile.map(line => line.split(" ")(1))
    val totalAge = ageData.map(age => Integer.parseInt(String.valueOf(age))).collect().reduce(_ + _)
    println("Total Age: " + totalAge + " ;Number of People: " + count)
    val avgAge: Double = totalAge.toDouble / count.toDouble
    println("Average Age is " + avgAge)
    sc.stop()
  }


}
