//将代码组织到cn.scala.chapter08包中
package cn.scala.chapter08

abstract class Animal {
  //抽象字段(域）
  var height:Int
  //抽象方法
  def eat:Unit
}

class Person(var height:Int) extends Animal{
  override def eat()={
    println("eat by mouth")
  }

}

object Person extends App{
  new Person(170).eat()
}