package basic

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Created by hushiwei on 2017/6/26.
  */
object configtest {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseFile(new File("src/main/resources/conf/application.conf"))

  }

}
