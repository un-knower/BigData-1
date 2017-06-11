
package cn.scala.chapter08

object AppDemo{
  //scala允许在任何地方进行包的引入，_的意思是引入该包下的所有类和对象
  import cn.scala._
  import cn.scala.chapter08._
  def main(args: Array[String]): Unit = {
    Utils.toString(new Teacher("john").name)
    new Teacher("john").printName()
  }

}
    //在包cn.scala下创建了一个Utils单例
    object Utils{
      def toString(x:String){
        println(x)
      }
      //外层包无法直接访问内层包，下面这一行代码编译通不过
      //def getTeacher():Teacher=new Teacher("john")
      //如果一定要使用的话，可以引入包
      import cn.scala.chapter08._
      def getTeacher():Teacher=new Teacher("john")
    }
class Teacher(var name:String) {
        //演示包的访问规则
        //内层包可以访问外层包中定义的类或对象，无需引入
        def printName()={Utils.toString(name)}
      }
