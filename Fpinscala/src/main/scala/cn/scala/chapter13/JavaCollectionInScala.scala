package cn.scala.chapter13

import java.util.ArrayList;
object JavaCollectionInScala extends App{
  def getList= {
    val list = new ArrayList[String]()
    list.add("Hadoop")
    list.add("Hive")
    list
  }
  val list=getList
  //引入scala.collection.JavaConversions._后
  //便可以使用Scala集合提供的一系列方法
  import scala.collection.JavaConversions._
  list.foreach(println)
  val list2=list.map(x=>x*2)
  println(list2)
}
