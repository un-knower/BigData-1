package gettingstarted

/**
  * Created by HuShiwei on 2016/8/24 0024.
  */
object demo {


  def main(args: Array[String]) {

    def fun(a:String): Unit ={
      println("hello")


    }
    fun("dfd")

    println("---------------")


    val list = List(1, 2, 3, 4, 5)
    println(list.head)
    println(list.tail)

    println(list)
    val result = list match {
      case _ => "hello"
      case List(1, _, _, _, _) => 1
    }

    println(result)
  }

}
