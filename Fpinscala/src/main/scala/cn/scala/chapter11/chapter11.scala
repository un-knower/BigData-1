package cn.scala.chapter11

/**
  * Scala泛型
  */
object ScalaExample11_1 extends App {

  //Person[T]中的[T]为指定的泛型T
  class Person[T](var name: T)

  class Student[T](name: T) extends Person(name)

  object GenericDemo {
    def main(args: Array[String]): Unit = {
      //在使用时将泛型参数具体化，这里为String类型
      println(new Student[String]("摇摆少年梦").name)
    }
  }

}


/**
  * Scala泛型：多个参数
  */
object ScalaExample11_2 extends App {

  class Person[T](var name: T)

  //多个泛型参数
  class Student[T, S](name: T, var age: S) extends Person(name)

  object GenericDemo {
    def main(args: Array[String]): Unit = {
      //使用时指定T为String类型、S为Int类型
      println(new Student[String, Int]("摇摆少年梦", 18).name)
    }
  }

}


/**
  * Scala存在类型
  */
object ScalaExample11_3 extends App {
  val arrStr: Array[String] = Array("Hadoop", "Hive", "Spark")
  val arrInt: Array[Int] = Array(1, 2, 3)
  printAll(arrStr)
  printAll(arrInt)

  //通过_简化设计，Array[_]与Array[T] forSome {type T}等价
  def printAll(x: Array[_]) = {
    for (i <- x) {
      print(i + " ")
    }
    println()
  }
}


/**
  * Scala存在类型的简化使用
  */
object ScalaExample11_4 extends App {
  val arrStr: Array[String] = Array("Hadoop", "Hive", "Spark")
  val arrInt: Array[Int] = Array(1, 2, 3)
  printAll(arrStr)
  printAll(arrInt)

  //通过_简化设计，Array[_]与Array[T] forSome {type T}等价
  def printAll(x: Array[_]) = {
    for (i <- x) {
      print(i + " ")
    }
    println()
  }
}


/**
  * 类型变量界定
  */
object ScalaExample11_5 extends App {

  class TypeVariableBound {
    //采用<:进行类型变量界定该语法的意思是泛型T必须是实现了Comparable接口的类型
    def compare[T <: Comparable[T]](first: T, second: T) = {
      if (first.compareTo(second) > 0)
        first
      else
        second
    }
  }

  val tvb = new TypeVariableBound
  println(tvb.compare("A", "B"))
}


/**
  * 类型变量界定
  */
object ScalaExample11_6 extends App {

  //声明Person类为case class且实现了Comparable接口
  case class Person(var name: String, var age: Int) extends Comparable[Person] {
    def compareTo(o: Person): Int = {
      if (this.age > o.age) 1
      else if (this.age == o.age) 0
      else -1
    }
  }

  class TypeVariableBound {
    def compare[T <: Comparable[T]](first: T, second: T) = {
      if (first.compareTo(second) > 0)
        first
      else
        second
    }
  }

  val tvb = new TypeVariableBound
  println(tvb.compare("A", "B"))
  //此时下面这条语句是合法的，因为Person类实现了Comparable接口
  println(tvb.compare(Person("stephen", 19), Person("john", 20)))

}

/**
  * 作用于类型参数
  */
object ScalaExample11_7 extends App {

  //定义Student类为case class，且泛型T的类型变量界定为AnyVal
  //在创建类时，所有处于AnyVal类继承层次结构的类都是合法的，如Int、Double等值类型
  case class Student[S, T <: AnyVal](var name: S, var hight: T)

  //下面这条语句是不合法的，因为String类型不属于AnyVal类层次结构
  // val S1=Student("john","170")
  //下面这两条语句都是合法的，因为Int,Long类型都是AnyVal
  val S2 = Student("john", 170.0)
  val S3 = Student("john", 170L)

}

/**
  * 不指定为视图界定时
  */
object ScalaExample11_8 extends App {

  //使用的是类型变量界定S <: Comparable[S]
  case class Student[T, S <: Comparable[S]](var name: T, var height: S)

  //可以编译通过，因为String类型在Comparable继承层次体系
  val s = Student("john", "170")
  //下面这条语句不合法，这是因为Int类型没有实现Comparable接口
  //val s2= Student("john",170)
}

/**
  * 指定为视图界定时
  */
object ScalaExample11_9 extends App {

  //利用<%符号对泛型S进行限定，它的意思是S可以是Comparable类继承层次结构
  //中实现了Comparable接口的类也可以是能够经过隐式转换得到的类,该类实现了Comparable接口
  case class Student[T, S <% Comparable[S]](var name: T, var height: S)

  val s = Student("john", "170")
  //下面这条语句在视图界定中是合法的因为Int类型此时会隐式转换为RichInt类，
  //而RichInt类实现了Comparable接口，属于Comparable继承层次结构
  val s2 = Student("john", 170)
}


/**
  * 上下文界定
  */
object ScalaExample11_11 extends App {

  //PersonOrdering混入了Ordering特质
  class PersonOrdering extends Ordering[Person] {
    override def compare(x: Person, y: Person): Int = {
      if (x.name > y.name)
        1
      else
        -1
    }
  }

  case class Person(val name: String) {
    println("正在构造对象:" + name)
  }

  //下面的代码定义了一个上下文界定
  //它的意思是在对应作用域中，必须存在一个类型为Ordering[T]的隐式值，该隐式值可以作用于内部的方法
  class Pair[T: Ordering](val first: T, val second: T) {
    //smaller方法中有一个隐式参数，该隐式参数类型为Ordering[T]
    def smaller(implicit ord: Ordering[T]) = {
      if (ord.compare(first, second) > 0)
        first
      else
        second
    }
  }

  //定义一个隐式值，它的类型为Ordering[Person]
  implicit val p1 = new PersonOrdering
  val p = new Pair(Person("123"), Person("456"))
  //不给函数指定参数，此时会查找一个隐式值，该隐式值类型为Ordering[Person]
  //根据上下文界定的要求，p1正好满足要求
  //因此它会作为smaller的隐式参数传入，从而调用ord.compare(first, second)方法进行比较
  println(p.smaller)

}


/**
  * 上下文界定
  */
object ScalaExample11_10 extends App {

  class PersonOrdering extends Ordering[Person] {
    override def compare(x: Person, y: Person): Int = {
      if (x.name > y.name)
        1
      else
        -1
    }
  }

  case class Person(val name: String) {
    println("正在构造对象:" + name)
  }

  class Pair[T: Ordering](val first: T, val second: T) {
    //引入odering到Ordered的隐式转换
    //在查找作用域范围内的Ordering[T]的隐式值
    //本例的话是implicit val p1=new PersonOrdering
    //编译器看到比较方式是<的方式进行的时候，会自动进行
    //隐式转换，转换成Ordered，然后调用其中的<方法进行比较
    import Ordered.orderingToOrdered;

    def smaller = {
      if (first < second)
        first
      else
        second
    }
  }

  implicit val p1 = new PersonOrdering
  val p = new Pair(Person("123"), Person("456"))
  println(p.smaller)
}


/**
  * 上下文界定
  */
object ScalaExample11_11 extends App {

  //PersonOrdering混入了Ordering特质
  class PersonOrdering extends Ordering[Person] {
    override def compare(x: Person, y: Person): Int = {
      if (x.name > y.name)
        1
      else
        -1
    }
  }

  case class Person(val name: String) {
    println("正在构造对象:" + name)
  }

  //下面的代码定义了一个上下文界定
  //它的意思是在对应作用域中，必须存在一个类型为Ordering[T]的隐式值，该隐式值可以作用于内部的方法
  class Pair[T: Ordering](val first: T, val second: T) {
    //smaller方法中有一个隐式参数，该隐式参数类型为Ordering[T]
    def smaller(implicit ord: Ordering[T]) = {
      if (ord.compare(first, second) > 0)
        first
      else
        second
    }
  }

  //定义一个隐式值，它的类型为Ordering[Person]
  implicit val p1 = new PersonOrdering
  val p = new Pair(Person("123"), Person("456"))
  //不给函数指定参数，此时会查找一个隐式值，该隐式值类型为Ordering[Person]
  //根据上下文界定的要求，p1正好满足要求
  //因此它会作为smaller的隐式参数传入，从而调用ord.compare(first, second)方法进行比较
  println(p.smaller)

}


/**
  * 多重界定
  */
object ScalaExample11_12 extends App {

  class A[T]

  class B[T]

  implicit val a = new A[String]
  implicit val b = new B[String]

  //多重上下文界定，必须存在两个隐式值，类型为A[T],B[T]类型
  //前面定义的两个隐式值a,b便是
  def test[T: A : B](x: T) = println(x)

  test("摇摆少年梦")

  implicit def t2A[T](x: T) = new A[T]

  implicit def t2B[T](x: T) = new B[T]

  //多重视图界定，必须存在T到A，T到B的隐式转换
  //前面我们定义的两个隐式转换函数就是
  def test2[T <% A[T] <% B[T]](x: T) = println(x)

  test2("摇摆少年梦2")
}


/**
  * 非变
  */
object ScalaExample11_13 extends App {

  //定义自己的List类
  class List[T](val head: T, val tail: List[T])

  //编译报错type mismatch; found :
  //cn.scala.chapter10.List[String] required:cn.scala.chapter10List[Any]
  //Note: String <: Any, but class List is invariant in type T.
  //You may wish to define T as +T instead. (SLS 4.5)
  val list: List[Any] = new List[String]("摇摆少年梦", null)
}

/**
  * 协变
  */
object ScalaExample11_13 extends App {

  //用+标识泛型T，表示List类具有协变性
  class List[+T](val head: T, val tail: List[T])

  //List泛型参数为协变之后，意味着List[String]也是List[Any]的子类
  val list: List[Any] = new List[String]("摇摆少年梦", null)
}

/**
  * 协变:逆变位置
  */
object ScalaExample11_13 extends App {

  class List[+T](val head: T, val tail: List[T]) {
    //将函数也用泛型表示，因为是协变的，函数参数是个逆变点，参数类型为T的超类
    def prepend[U >: T](newHead: U): List[U] = new List(newHead, this)

    override def toString() = "" + head
  }

  val list: List[Any] = new List[String]("摇摆少年梦", null)
}

/**
  * 逆变
  */
object ScalaExample11_13 extends App {

  class Person[-A] {
    def test(x: A): Unit = {
      println(x)
    }
  }

  val pAny = new Person[Any]
  //根据里氏替换原则，子类Person[Any]可以赋值给父类Person[String]
  val pStr: Person[String] = pAny
  pStr.test("Contravariance test----")
}

/**
  * 逆变：协变位置
  */
object ScalaExample11_13 extends App {

  class Person [-A]{
    def test(x:A): Unit ={
      println(x)
    }
    //下面这行代码会编译出错
    //contravariant type A occurs in covariant position in type ⇒ A of method test
    def test(x:A):A=null.asInstanceOf[A]
  }

}


/**
  * 未使用单例类型时的链式调用
  */
object Example11_14 extends App{
  class Pet{
    private var name:String=null
    private var weight:Float=0.0f

    def setName(name:String)={
      this.name=name
      //返回调用对象，类型为Pet
      this
    }
    def setWeight(weight:Float)={
      this.weight=weight
      //返回调用对象，类型为Pet
      this
    }
    override  def toString=s"name=$name,age=$weight"
  }

  class Dog extends Pet{
    private var age:Int=0
    def setAge(age:Int)={
      this.age=age
      //返回调用对象，类型为Dog
      this
    }
    override  def toString=super.toString+s",age=$age"
  }

  //代码能够顺利执行
  println(new Dog().setAge(2).setName("Nacy").setWeight(20.0f))

  //编译报错，Error:(33, 54) value setAge is not a member of cn.scala.chapter10.Example11_11.Pet
  //println(new Dog().setName("Nacy").setWeight(20.0f).setAge(2))
}

/**
  * 使用单例类型时的链式调用
  */
object Example11_15 extends App{

  class Pet{
    private var name:String=null
    private var weight:Float=0.0f
    //将函数返回值类型定义为单例类型
    def setName(name:String):this.type={
      this.name=name
      //返回调用对象，类型为Pet
      this
    }

    //将函数返回值类型定义为单例类型
    def setWeight(weight:Float):this.type={
      this.weight=weight
      //返回调用对象，类型为Pet
      this
    }
    override  def toString=s"name=$name,age=$weight"
  }

  class Dog extends Pet{
    private var age:Int=0

    //将函数返回值类型定义为单例类型
    def setAge(age:Int):this.type ={
      this.age=age
      //返回调用对象，类型为Dog
      this
    }
    override  def toString=super.toString+s",age=$age"
  }

  //代码能够顺利执行
  println(new Dog().setAge(2).setName("Nancy").setWeight(20.0f))

  //同样能够顺利执行
  println(new Dog().setName("Nancy").setWeight(20.0f).setAge(2))
}

/**
  * 类型投影
  */
object Example11_16 extends App{

  class Outter{
    val x:Int=0
    def test(i:Outter#Inner)=i
    class Inner
  }

  val o1=new Outter
  val o2=new Outter

  val inner2=new o2.Inner
  val inner1=new o1.Inner

  //编译通过
  o1.test(inner1)
  //编译通过
  o1.test(inner2)
}

/**
  * 抽象类型
  */
object Example11_17 extends App{

  abstract class Food
  class Rice extends Food{
    override def toString="粮食"
  }
  class Meat extends Food{
    override def toString="肉"
  }

  class Animal{
    //定义了一抽象类型FoodType
    type FoodType
    //函数参数类型为FoodType
    def eat(f:FoodType)=f
  }
  class Human extends Animal{
    //子类中确定其具体类型为Food
    type FoodType=Food
    //函数参数类型为FoodType，因为已经具体化了，所以为Food
    override  def eat(f:FoodType)=f

  }
  class Tiger extends Animal{
    //子类中确定其具体类型为Meat
    type FoodType=Meat
    //函数参数类型为FoodType，因为已经具体化了，所以为Meat
    override  def eat(f:FoodType)=f
  }
  val human=new Human
  val tiger=new Tiger
  println("人可以吃："+human.eat(new Rice))
  println("人可以吃："+human.eat(new Meat))
  println("老虎只能吃："+tiger.eat(new Meat))
}

