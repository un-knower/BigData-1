package SomethingAboutExamples.SogouQ

/**
  * Created by HuShiwei on 2016/9/7 0007.
  */
@SerialVersionUID(100L)
class SogouAccessLogParser extends Serializable {
  def parseRecord(line: String): Option[SogouAccessLogRecord] = {
    val arr = line.split("\t")
    if (arr.length == 4) {
      Some(buildAccessLogRecord(arr))
    } else {
      None
    }

  }

  private def buildAccessLogRecord(arr: Array[String]) = {
    val orderAndRank = arr(2).split(" ")
    SogouAccessLogRecord(arr(0), arr(1), orderAndRank(0), orderAndRank(1), arr(3))

  }


}

object SogouLogParser{
  val nullObjectSogouLogRecord=SogouAccessLogRecord("","","","","")
  def getWord(line:String):String={
    val word=line.substring(line.indexOf("[")+1,line.indexOf("]"))
    word
  }

//  00:00:20	11515839301781111	[最新2008年运程]	4 4	bbs.club.sohu.com/rs-aries-74-267001-226426-1205714423.html
  def main(args: Array[String]) {
  //  val line="11515839301781111\t[最新2008年运程]\t4 4\tbbs.club.sohu.com/rs-aries-74-267001-226426-1205714423.html"
  val line = "hello\t11515839301781111\t[最新2008年运程]\t4 4\tbbs.club.sohu.com/rs-aries-74-267001-226426-1205714423.html"
  val parser = new SogouAccessLogParser()
  val record = parser.parseRecord(line)
  println(record)
  val aa=println(record.getOrElse("ddddddddd"))
  if (record==None) {
    println("sssss")

  }
  //  println(record.get.query)
  //  println(getWord(record.get.query))
}
}
