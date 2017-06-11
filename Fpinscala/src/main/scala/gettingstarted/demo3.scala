package gettingstarted

/**
  * Created by HuShiwei on 2016/11/16 0016.
  */
object demo3 {
  def main(args: Array[String]) {
    val arr=0 until(10,3)
    arr.toArray.foreach(print)

    val ss=Range.apply(1,10)
    ss.foreach(print)


    println("-----------")

    for(i<- 0 until 10)
      println(i)
  }

}
