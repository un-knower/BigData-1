/**
  * Created by hushiwei on 2017/4/14.
  */
object demod {
  def main(args: Array[String]): Unit = {
    var map:Map[String,String]=Map()
    map+=("hello9"->"spark")

    //    val str=map.get("hello").map(_.toUpperCase)
    val str=map.getOrElse("hello",4)


    val num=map.get("hello").getOrElse(0)



    val aa:Option[String] =null

    if (null==aa) {
      println("aa is null")
    }


    val orElse = aa.getOrElse("bb")
    if (orElse.equals("bb")){
      println("hello world..")
    }


  }

}
