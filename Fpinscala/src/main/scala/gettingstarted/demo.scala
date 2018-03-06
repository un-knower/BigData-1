package gettingstarted

import scala.io.Source

/**
  * Created by HuShiwei on 2016/8/24 0024.
  */
object demo {


  def main(args: Array[String]) {

    val log = "/Users/hushiwei/IdeaProjects/BigData/Fpinscala/src/main/resources/demo1.txt"
    val session = "/Users/hushiwei/IdeaProjects/BigData/Fpinscala/src/main/resources/demo2.txt"
    val file = Source.fromFile(log).getLines().toList
    val sessionIds = Source.fromFile(session).getLines().toList

    var result: Set[String] = Set()

    for (line <- file) {
      for (se <- sessionIds) {

        if (!line.contains(se.trim)) {
          result += line

        }
      }
    }

    for (line <- result) {
      println("--->" + line)
    }
  }

}
