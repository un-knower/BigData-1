package SomethingAboutExamples.LogProcessApacheLog

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

/**
  * Created by HuShiwei on 2016/5/18.
  */

/**
  * A sample record:
  * 94.102.63.11 - - [21/Jul/2009:02:48:13 -0700] "GET / HTTP/1.1" 200 18209 "http://acme.com/foo.php" "Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)"
  *
  * I put this code in the 'class' so (a) the pattern could be pre-compiled and (b) the user can create
  * multiple instances of this parser, in case they want to work in a multi-threaded way.
  * I don't know that this is necessary, but I think it is for this use case.
  *
  */
@SerialVersionUID(100L)
class AccessLogParser extends Serializable{
  private val ddd = "\\d{1,3}"                      // at least 1 but not more than 3 times (possessive)
  private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  // like `123.456.7.89`
  private val client = "(\\S+)"                     // '\S' is 'non-whitespace character'
  private val user = "(\\S+)"
  private val dateTime = "(\\[.+?\\])"              // like `[21/Jul/2009:02:48:13 -0700]`
  private val request = "\"(.*?)\""                 // any number of any character, reluctant
  private val status = "(\\d{3})"
  private val bytes = "(\\S+)"                      // this can be a "-"
  private val regex = s"$ip $client $user $dateTime $request $status $bytes"
  private val p = Pattern.compile(regex)

  def parseRecord(record: String):Option[AccessLogRecord]={
    val matcher= p.matcher(record)
    if (matcher.find){
      Some(buildAccessLogRecord(matcher))
    }else{
      None
    }
  }

  def parseRecordReturningNullObjectOnFailure(record: String):AccessLogRecord= {
    val matcher = p.matcher(record)
    if (matcher.find) {
      buildAccessLogRecord(matcher)
    }else{
      AccessLogParser.nullObjectAccessLogRecord
    }
  }

  private def buildAccessLogRecord(matcher:Matcher)={
    AccessLogRecord(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7))
  }
}

/**
  * A sample record:
  * 94.102.63.11 - - [21/Jul/2009:02:48:13 -0700] "GET / HTTP/1.1" 200 18209 "http://acme.com/foo.php" "Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)"
  */
object AccessLogParser{
  val nullObjectAccessLogRecord= AccessLogRecord("", "", "", "", "", "", "")

  def parseRequestField(request:String):Option[Tuple3[String,String,String]]={
    val arr= request.split(" ")
    if (arr.size==3) Some((arr(0),arr(1),arr(2))) else None
  }

  def parseDateField(field:String):Option[java.util.Date]={
    val dateRegex = "\\[(.*?) .+]"
    val datePattern= Pattern.compile(dateRegex)
    val dateMatcher=datePattern.matcher(field)
    if(dateMatcher.find) {
      val dateString= dateMatcher.group(1)
      println("****** DATE STRING"+ dateString)
      // HH is 0-23 ; KK is 1-24
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)

      Some(dateFormat.parse(dateString))
    }else {
      None
    }
  }

  def main(args: Array[String]) {
    val acc = new AccessLogParser()
//    val str="94.102.63.11 - - [21/Jul/2009:02:48:13 -0700] \"GET / HTTP/1.1\" 200 18209 \"http://acme.com/foo.php\" \"Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)\""
    val str="64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846"
    val record = acc.parseRecord(str)
    val pp=record.get
    val date = pp.dateTime
    val datestr = parseDateField(date)
    println(datestr.get.toString)

    println(parseRequestField(pp.request))
  }
}
