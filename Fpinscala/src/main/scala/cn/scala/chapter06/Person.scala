package cn.scala.chapter06

import scala.beans.BeanProperty

/**
  * Created by 周志湖 on 2016/3/5.
  */
class Person {
  private[this] var name:String=null
  def print=println(s"$name")
}

//class Student extends Person{
// // override def toString: String = this.name
//}

object Student {
  private var studentNo:Int=0;
  def uniqueStudentNo()={
    studentNo+=1
    studentNo
  }
}
