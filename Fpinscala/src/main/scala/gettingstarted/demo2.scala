package gettingstarted
import Array._
/**
  * Created by HuShiwei on 2016/11/14 0014.
  */
object demo2 {
  def main(args: Array[String]) {
//    val map:Map[String,String]=Map{"hello"->"world"}
    var map:Map[String,String]=Map()
    map+=("bidreq_app_cat"->"null")

//    val str=map.get("hello").map(_.toUpperCase)
    val str=map.getOrElse("hello",4)

    println(str)

//    var myMatrix=ofDim[Int](3,3)
//
//    var list=1::2::3::Nil
////    list+=4
//    val ar1=list.::(6)
//    println(ar1)
//    println(list)
//    import scala.collection.mutable.Map
//    val map1=Map(1->2)
//    val map2=map1.+(2->3)
//    map1.+=(3->4,5->6)
//    println(map2)
//    println(map1)
//
//    println("--------------------")
//    val colors = Map("red" -> "#FF0000",
//      "azure" -> "#F0FFFF",
//      "peru" -> "#CD853F")
//
//    println("第一种遍历")
//    colors.keys.foreach{key=>
//    print("key= "+key)
//    println(" value= "+colors(key))}
//
//    println("第二种遍历")
//
//    for(key <- colors.keySet.toArray) {
//      println(key+" :　"+colors.get(key))
//    }
//
//    println("第三种遍历")
//
//    colors.map{
//      case (k,v)=>println(k+" : "+v)
//    }
//    println("----------tuple----------")
//    val t = (4,3,2,1)
//
//    t.productIterator.foreach{ i =>println("Value = " + i )}

  }

}
