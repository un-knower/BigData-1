/**
  * Created by hushiwei on 2017/4/14.
  */
object demod {
  def main(args: Array[String]): Unit = {
    var map:Map[String,String]=Map()
    map+=("hello9"->"spark")

    //    val str=map.get("hello").map(_.toUpperCase)
    val str=map.getOrElse("hello",4)

    println(str)

    val num=map.get("hello").getOrElse(0)
    println(num)

  }

}
