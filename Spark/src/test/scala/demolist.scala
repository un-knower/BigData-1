import scala.collection.mutable.ListBuffer

/**
  * Created by hushiwei on 2017/8/17.
  */
object demolist {
  def main(args: Array[String]): Unit = {
    val list1 = ListBuffer[String]()
    list1+="1"
    list1+="2"
    list1+="3"
    list1+="4"

    list1.foreach(println)
    println("---------------------------")

    val list2 = ListBuffer[String]()

    list2+="6"
    list2+="7"
    list2+="8"
    list2+="9"

    list2.foreach(println)

    println("---------------------------")

    list1.++=:(list2)
    list1.foreach(println)





  }

}
