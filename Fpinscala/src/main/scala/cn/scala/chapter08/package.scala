//下面的代码给出了包对象的定义
package cn.scala.chapter08

//利用package关键字定义单例对象
package object Math {
  val PI=3.141529
  val THETA=2.0
  val SIGMA=1.9
}

class Coputation{
  def computeArea(r:Double)=Math.PI*r*r
}