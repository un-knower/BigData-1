package SomethingAboutExamples.SparkDataProcess.DataProcess03

import java.io.FileWriter

import scala.util.Random

/**
  * Created by HuShiwei on 2016/6/8 0008.
  */

/**
  * 6 个列 (ID, 性别, 年龄, 注册日期, 角色 (从事行业), 所在区域) 的文本文件
  *
  * 1 F 21 2006-10-1 ROLE005 REG002
  * 2 F 30 2001-10-7 ROLE002 REG004
  * 3 M 43 2004-5-23 ROLE003 REG005
  * 4 F 13 2012-12-27 ROLE004 REG003
  * 5 M 14 2015-11-9 ROLE0001 REG002
  * 6 F 38 2004-2-19 ROLE003 REG001
  * 7 M 46 2004-9-20 ROLE003 REG003
  * 8 F 50 2005-9-15 ROLE003 REG003
  * 9 F 53 2009-5-16 ROLE005 REG003
  * 10 F 15 2005-7-1 ROLE003 REG001
  */
object UserDataGenerator {
  private val FILE_PATH = "E:\\bigdataData\\sample_user_data.txt"
  private val ROLE_ID_ARRAY = Array[String]("ROLE0001", "ROLE002", "ROLE003", "ROLE004", "ROLE005")
  private val REGION_ID_ARRAY = Array[String]("REG001", "REG002", "REG003", "REG004", "REG005")
  private val MAX_USER_AGE = 60
  //  how many records to be generated
  private val MAX_RECORDS = 10000000

  def main(args: Array[String]) {
    generateDataFile(FILE_PATH, MAX_RECORDS)
  }

  private def generateDataFile(filePath: String, recordNum: Int): Unit = {
    var writer: FileWriter = null
    try {
      writer = new FileWriter(filePath, true)
      val rand = new Random()
      for (i <- 1 to recordNum) {
        //      generate the gender of the user
        val gender = getRandomGender()
        var age = rand.nextInt(MAX_USER_AGE)
        if (age < 10) {
          age = age + 10
        }
        //      generate the registering date for user
        var year = rand.nextInt(16) + 2000
        var month = rand.nextInt(12) + 1
        //      to avoid checking if it is a valid day for specific month
        //      we always generate a day which is no more than 28
        var day = rand.nextInt(28) + 1
        var registerDate = year + "-" + month + "-" + day
        //      generate the role of the user
        var roleIndex = rand.nextInt(ROLE_ID_ARRAY.length)
        var role = ROLE_ID_ARRAY(roleIndex)
        //      generate the region where the user is
        var regionIndex = rand.nextInt(REGION_ID_ARRAY.length)
        var region = REGION_ID_ARRAY(regionIndex)

        writer.write(i + " " + gender + " " + age + " " + registerDate
          + " " + role + " " + region)
        writer.write(System.getProperty("line.separator"))
      }
      writer.flush()
    } catch {
      case e: Exception => println("Error occurred:" + e)
    } finally {
      if (writer != null)
        writer.close()
    }
    println("User Data File generated successfully.")
  }

  private def getRandomGender(): String = {
    val rand = new Random()
    val randNum = rand.nextInt(2) + 1
    if (randNum % 2 == 0) {
      "M"
    } else {
      "F"
    }
  }

}
