package cn.scala.chapter10

import scala.collection.immutable.Range
import scala.runtime.{RangedProxy, ScalaNumberProxy}

/*
*隐式对象
*/
object Example10_1 extends  App{

  //定义一个trait Multiplicable
  trait Multiplicable[T] {
    def multiply(x: T): T
  }

  //定义一个隐式对象MultiplicableInt，用于整型数据的相乘
    implicit object MultiplicableInt extends Multiplicable[Int] {
      def multiply(x: Int) = x*x
    }
  //定义一个隐式对象MultiplicableString，用于字符串数据的乘积
    implicit object MultiplicableString extends Multiplicable[String] {
      def multiply(x: String) = x*2
    }

  //定义一个函数，函数具有泛型参数
  def multiply[T: Multiplicable](x:T) = {
    //implicitly方法，访问隐式对象
    val ev = implicitly[Multiplicable[T]]
    //根据具体的类型调用相应的隐式对象中的方法
    ev.multiply(x)
  }

  //调用隐式对象MultiplicableInt中的multiply方法
  println(multiply(5))
  //调用隐式对象MultiplicableString中的multiply方法
  println(multiply("5"))

}



/*
*隐式参数
*/
object Example10_2 extends  App{

  //定义一个trait Multiplicable
  trait Multiplicable[T] {
    def multiply(x: T): T
  }

  //定义一个隐式对象MultiplicableInt，用于整型数据的相乘
  implicit object MultiplicableInt extends Multiplicable[Int] {
    def multiply(x: Int) = x*x
  }
  //定义一个隐式对象MultiplicableString，用于字符串数据的乘积
  implicit object MultiplicableString extends Multiplicable[String] {
    def multiply(x: String) = x*2
  }

  //使用隐式参数定义multiply函数
  def multiply[T: Multiplicable](x:T)(implicit  ev:Multiplicable[T]) = {
      //根据具体的类型调用相应的隐式对象中的方法
    ev.multiply(x)
  }

  //调用隐式对象MultiplicableInt中的multiply方法
  println(multiply(5))
  //调用隐式对象MultiplicableString中的multiply方法
  println(multiply("5"))

}


/*
*使用隐式值对Example10_2进行改造
*/
object Example10_3 extends  App{

  //定义一个trait Multiplicable
  trait Multiplicable[T] {
    def multiply(x: T): T
  }

  //定义一个普通类MultiplicableInt，用于整型数据的相乘
  class MultiplicableInt extends Multiplicable[Int] {
    def multiply(x: Int) = x*x
  }
  //定义一个普通类MultiplicableString，用于字符串数据的乘积
  class MultiplicableString extends Multiplicable[String] {
    def multiply(x: String) = x*2
  }

  //使用隐式参数定义multiply函数
  def multiply[T: Multiplicable](x:T)(implicit  ev:Multiplicable[T]) = {
    //根据具体的类型调用相应的隐式对象中的方法
    ev.multiply(x)
  }

  //类型为MultiplicableInt的隐式值mInt
  implicit val mInt=new MultiplicableInt

  //类型为MultiplicableString的隐式值mStr
  implicit val mStr=new MultiplicableString

  //隐式值mInt当作参数传入ev，给相当于调用multiply(5)(mInt)
  println(multiply(5))
  //隐式值mStr当作参数传入ev，相当于调用multiply(5)(mStr)
  println(multiply("5"))

}


/*
* 多次隐式转换
*/
object Example10_4 extends  App{

  class TestA{
    override def toString="this is TestA"
    def printA=println(this)
  }

  class TestB{
    override def toString="this is TestB"
    def printB(x:TestC)=println(x)
  }

  class TestC{
    override def toString="this is TestC"
    def printC=println(this)
  }

  //TestA到TestB的隐式转换函数
  implicit def A2B(x:TestA)={
    println("TestA is being converted to TestB")
    new TestB
  }

  //TestB到TestC的隐式转换函数
  implicit def B2C(x:TestB)={
    println("TestB is being converted to TestC")
    new TestC
  }

  val a=new TestA
  //TestA中不会存在printB方法，因此会尝试隐式转换，调用的是implicit def A2B(x:TestA)，这是第一次隐式转换
  //在执行printB方法时，由于def printB(x:TestC)接受的参数类型为TestC，与实际不匹配，因此会尝试隐式转换
  //调用的是implicit def A2B(x:TestA)，这是第二次隐式转换
  a.printB(new TestB)
}

