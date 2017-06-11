package cn.scala.chapter13

import java.util._

case class Person(val name:String,val age:Int)
//在Java中Comparator是这么用的：Comparator<Person>
//而在Scala中是这么用的：Comparator[Person]
class PersonComparator extends Comparator[Person]{
  override def compare(o1: Person, o2: Person): Int = if(o1.age>o2.age) 1 else -1
}
/**
  * Java泛型在Scala中的使用示例
  */
object JavaGenericInScala extends  App{
  val p1=Person("摇摆少年梦",27)
  val p2=Person("李四",29)
  val personComparator=new PersonComparator()
  if(personComparator.compare(p1,p2)>0) println(p1)
  else println(p2)
}
