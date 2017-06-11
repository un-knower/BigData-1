package cn.scala.chapter06


/**
  * 应用程序对象
  */

object Example6_01 extends  App{
  object Student {
    private var studentNo:Int=0;
    def uniqueStudentNo()={
      studentNo+=1
      studentNo
    }
  }
  println(Student.uniqueStudentNo())

}

/**
  * 伴生对象与伴生类
  */
object Example6_02 extends  App{
  //伴生类Student
  class Student{
    private var name:String=null
    def getStudentNo={
      Student.uniqueStudentNo()
      //伴生类Student中可以直接访问伴生对象Student的私有成员
      Student.studentNo
    }
  }

  //伴生对象
  object Student {
    private var studentNo:Int=0;
    def uniqueStudentNo()={
      studentNo+=1
      studentNo
    }
    //直接访问伴生类对象的私有成员
    def printStudenName=println(new Student().name)
  }
  //不允许直接访问伴生对象Student的私有成员studentNo
  //println(Student.studentNo)

  //不允许访问伴生类Student对象的私有成员name
  //println(new Student().name)

  println(new Student().getStudentNo)
  println(Student.printStudenName)
 Array(1,2,3,4,5)

}


/**
  * 伴生对象与伴生类:不显式地通过new关键字创建对象
  */
object Example6_03 extends  App{
  //伴生类Student
  class Student{
    var name:String=null
  }

  //伴生对象
  object Student {
    def apply()=new Student()
  }

  val s=Student()
  s.name="John"
  println(s.name)

}

/**
  * 主构造器
  */
object Example6_04 extends  App{

  //默认参数的主构造器
  class Person private(val name:String="john",val age:Int)

}


/**
  * 辅助构造函数
  */
object Example6_05 extends  App{

  //定义无参的主构造函数
  class Person{
    //类成员
    private var name:String=null
    private var age:Int=18
    private var sex:Int=0

    //辅助构造函数
    def this(name:String){
      //this()调用的是无参的默认主构造函数
      this()
      this.name=name
    }
    def this(name:String,age:Int){
      //this(name)调用的是前面定义的辅助构造函数def this(name:String)
      this(name)
      this.age=age
    }
    def this(name:String,age:Int,sex:Int){
      //this(name)调用的是前面定义的辅助构造函数def this(name:String,age:Int)
      this(name,age)
      this.sex=sex
    }

    override def toString = {
      val sexStr=if(sex==1) "男" else "女"
      s"name=$name,age=$age,sex=$sexStr"
    }
  }

  //调用的是三个参数的辅助构造函数
  println(new Person("John",19,1))
  //调用的是二个参数的辅助构造函数
  println(new Person("John",19))
  //调用的是一个参数的辅助构造函数
  println(new Person("John"))

}


/**
  * 辅助构造函数:默认参数
  */
object Example6_06 extends  App{

  //定义无参的主构造函数
  class Person{
    //类成员
    private var name:String=null
    private var age:Int=18
    private var sex:Int=0

    //带默认参数的主构造函数
    def this(name:String="",age:Int=18,sex:Int=1){
      //先调用主构造函数，这是必须的，否则会报错
      this()
      this.name=name
      this.age=age
      this.sex=sex
    }

    override def toString = {
      val sexStr=if(sex==1) "男" else "女"
      s"name=$name,age=$age,sex=$sexStr"
    }
  }

  println(new Person("John"))
  //注意这里调用的是无参的主构造函数，而不是带有默认参数的辅助构造函数
  println(new Person)
}


/**
  * 辅助构造函数:将主构造函数定义为private
  */
object Example6_07 extends  App{

  //定义无参的主构造函数并将其定义为私有
  class Person private{
    //类成员
    private var name:String=null
    private var age:Int=18
    private var sex:Int=0

    //带默认参数的主构造函数
    def this(name:String="",age:Int=18,sex:Int=1){
      //先调用主构造函数，这是必须的，否则会报错
      this()
      this.name=name
      this.age=age
      this.sex=sex
    }

    override def toString = {
      val sexStr=if(sex==1) "男" else "女"
      s"name=$name,age=$age,sex=$sexStr"
    }
  }
  //使用默认参数，调用的是辅助构造函数，相当于调用Person(“John”,18,1)
  println(new Person("John"))
  //由于主构造函数为private，此时调用是便是带默认参数的辅助构造函数
  //相当于调用Person("",18,1)
  println(new Person)
}


/**
  * 类继承
  */
object Example6_08 extends  App{

  //定义Person类，带主构造函数
  class Person(var name:String,var age:Int){
    override def toString: String = "name="+name+",age="+age
  }

  //通过extends关键字实现类的继承，注意Student中的name和age前面没有var关键字修饰
  //表示这两个成员继承自Person类，var studentNo:String则为Student类中定义的新成员
  class Student(name:String,age:Int,var studentNo:String) extends Person(name,age){
    override def toString: String = super.toString+",studentNo="+studentNo
  }

  println(new Person("John",18))
  println(new Student("Nancy",19,"140116"))
}


/**
  * 类继承：构造函数执行顺序
  */
object Example6_09 extends  App{

  //定义Person类，带主构造函数
  class Person(var name:String,var age:Int){
    println("执行Person类的主构造函数")
  }


  class Student(name:String,age:Int,var studentNo:String) extends Person(name,age){
    println("执行Student类的主构造函数")
  }

  //创建子类对象时，先调用父类的主构造函数，然后再调用子类的主构造函数
  new Student("Nancy",19,"140116")

}


/**
  * 类继承：方法重写
  */
object Example6_10 extends  App{

  class Person(var name:String,var age:Int){
    //对父类Any中的toString方法进行重写
    override def toString = s"Person($name, $age)"
  }

  class Student(name:String,age:Int,var studentNo:String) extends Person(name,age){
    //对父类Person中的toString方法进行重写
    override def toString = s"Student($name,$age,$studentNo)"
  }

  //调用Student类自身的toString方法返回结果
  println(new Student("Nancy",19,"140116"))
}


/**
  * 类继承：动态绑定与多态
  */
object Example6_11 extends App {

  //抽象Person类
  class Person(var name: String, var age: Int) {
    def walk(): Unit=println("walk() method in Person")
    //talkTo方法，参数为Person类型
    def talkTo(p: Person): Unit=println("talkTo() method in Person")
  }

  class Student(name: String, age: Int) extends Person(name, age) {
    private var studentNo: Int = 0

    //重写父类的talkTo方法
    override def talkTo(p: Person) = {
      println("talkTo() method in Student")
      println(this.name + " is talking to " + p.name)
    }
  }

  class Teacher(name: String, age: Int) extends Person(name, age) {
    private var teacherNo: Int = 0

    //重写父类的walk方法
    override def walk() = println("walk() method in Teacher")

    //重写父类的talkTo方法
    override def talkTo(p: Person) = {
      println("talkTo() method in Teacher")
      println(this.name + " is talking to " + p.name)
    }
  }


  //下面的两行代码演示了多态的使用,Person类的引用可以指向Person类的任何子类
  val p1: Person = new Teacher("albert", 38)
  val p2: Person = new Student("john", 38)


  //talkTo方法参数类型为Person类型,p1.talkTo(p2)传入的实际类型是Student
  //p2.talkTo(p1)传入的实际类型是Teacher,程序会根据实际类型调用对应的不同子类中的talkTo()方法
  p1.walk()
  p1.talkTo(p2)
  println("/////////////////////////////")
  p2.walk()
  p2.talkTo(p1)

}


/**
  * 抽象类
  */
object Example6_12 extends App {
  abstract class Person{
    //抽象类中的具体成员变量
    var age:Int=0
    //抽象类中的抽象成员变量
    var name:String
    //抽象类中的抽象方法
    def walk()
    //抽象类中的具体方法
    override def toString=name
  }

  class Student extends Person{
    //对继承自父类的抽象成员变量进行初始化，override关键字可以省略
    override var name: String = _

    //对继承父类的抽象成员方法进行初始化，override关键字可以省略
    override def walk(): Unit = println("Walk like a Student")
  }

  val p=new Student
  p.walk()
}


/**
  * 内部类与对象
  */
object Example6_13 extends App {

  //伴生类Student
  class Student(var name:String,var age:Int){
    //内部类Grade
    class Grade(var name:String)

    //内部对象
    object Utils1{
      def print(name:String)=println(name)
    }
  }


  //伴生对象Student
  object Student{
    //单例对象中的内部类Printer
    class Printer{
      def print(name:String)=println(name)
    }

    //伴生对象中的单例对象
    object Utils2{
      def print(name:String)=println(name)
    }
  }

  val student=new Student("John",18)
  //创建伴生类的内部类对象
  val grade=new student.Grade("大学一年级")
  println("new student.Grade(\"大学一年级\").name：调用伴生类的内部类方法，grade.name="+grade.name)

  //调用伴生类的内部对象方法
  student.Utils1.print("student.Utils1.print：调用伴生类的内部对象方法")

  //创建伴生对象的内部类对象
  val printer=new Student.Printer()
  printer.print("new Student.Printer().print: 调用伴生对象的内部类方法")

  //调用伴生对象的内部对象方法
  Student.Utils2.print("Student.Utils2.print：调用伴生对象的内部对象方法")

}


/**
  * 匿名类
  * */
object Example6_14 extends App {

  abstract class Person(var name:String,var age:Int){
    def print:Unit
  }

  //使用匿名类,并创建匿名类对象
  val p=new Person("John",18) {
    override def print: Unit = println(s"Person($name,$age)")
  }

//  class NamedClass(name:String,age:Int) extends  Person(name,age){
//    override def print: println(s"Person($name,$age)")
//  }
//  val p=new NamedClass("John",18)
  p.print

}


/**
  * private关键字修饰的成员变量:倖生类与伴生对象中可以访问
  * */
object Example6_15 extends App {

  class Person{
    //private修饰的成员变量
    private var name:String=_
    def this(name:String){
      this();
      this.name=name
    }
  }

  object Person{
    //在伴生对象中可以访问伴生类对象的private成员变量
    def printName=println(new Person("John").name)
  }

}



/**
  * private[this]修饰的成员变量，只能在伴生类中访问，在伴生对象及外部不能访问
  * */
object Example6_16 extends App {

  class Person{
    //private[this]修饰的成员变量
    private[this] var name:String=_
    def this(name:String){
      this();
      this.name=name
    }
    //在本类中可以访问
    def print=println(name)
  }

  object Person{
    //在伴生对象中不能访问
    //def printName=println(new Person("John").name)
  }

  //在伴生对象和伴生类的外部不能访问private[this]成员变量
  //println(new Person("John").name)
}



/**
  * 无伴生对象，成员变量为private
  * */
object Example6_17 extends App {

  class Person{
    //private修饰的成员变量
    private var name:String=_
    def this(name:String){
      this();
      this.name=name
    }
  }
//  object Person{
//    //在伴生对象中可以访问伴生类对象的private成员变量
//    def printName=println(new Person("John").name)
//  }

}


/**
  * 有伴生对象，成员变量为private
  * */
object Example6_18 extends App {

  class Person{
    //private修饰的成员变量
    private var name:String=_
    def this(name:String){
      this();
      this.name=name
    }
  }
    object Person{
      //在伴生对象中可以访问伴生类对象的private成员变量
      def printName=println(new Person("John").name)
    }

}


/**
  * 无伴生对象，有private[this]成员变量
  * */
object Example6_19 extends App {

  class Person{
    //private[this]修饰的成员变量
    private[this] var name:String=_
    def this(name:String){
      this();
      this.name=name
    }
  }

}


/**
  * 有伴生对象，有private[this]成员变量
  * */
object Example6_20 extends App {

  class Person{
    //private[this]修饰的成员变量
    private[this] var name:String=_
    def this(name:String){
      this();
      this.name=name
    }
  }
  object Person{

  }
}


/**
  * 有伴生对象，有private[this]成员变量
  * */
object Example6_21 extends App {

  //在主构造函数中定义private类成员变量
  class Person(var name:String, var age:Int)

  class Student(name:String,age:Int,var studentNo:String) extends Person(name,age)

  val p=new Person("John",18)
  println(p)

//  class Person{
//    protected var name:String=_
//    var age:Int=_
//    def this(name:String,age:Int){
//      this()
//      this.name=name
//      this.age=age
//    }
//  }

}