package SomethingAboutExamples.SparkDataProcess.DataProcess02

import java.io.{File, FileWriter}

import scala.util.Random

/**
  * Created by HuShiwei on 2016/6/8 0008.
  */

/**
  * 某个省的人口 (1 亿) 性别还有身高进行统计
  * 三列分别是 ID，性别，身高 (cm)。
  * 1 M 174
  * 2 F 165
  * 3 M 173
  * ...
  */
object PeopleInfoFileGenerator {
  def main(args: Array[String]) {
    val writer = new FileWriter(new File("E:\\bigdataData\\sample_people_info.txt"), false)
    val rand = new Random()
    for (i <- 1 to 100000000) {
      var height = rand.nextInt(220)
      if (height < 50) {
        height = height + 50
      }
      val gender = getRandomGender
      if (height < 100 && gender == "M")
        height = height + 100
      if (height < 100 && gender == "F")
        height = height + 50
      writer.write(i + " " + getRandomGender() + " " + height)
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
    println("People Information File generated successfully.")

  }

  def getRandomGender(): String = {
    val rand = new Random()
    val randNum = rand.nextInt(2) + 1
    if (randNum % 2 == 0) {
      "M"
    } else {
      "F"
    }
  }

}
