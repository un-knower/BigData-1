package cn.scala.chapter07

/**
  * 只有抽象方法的trait
  */
object Example07_01 extends App {

  case class Person(var id: Int, var name: String, var age: Int)

  //定义只包含抽象方法的trait,
  trait PersonDAO {
    //添加方法
    def add(p: Person)

    //更新方法
    def update(p: Person)

    //删除方法
    def delete(id: Int)

    //查找方法
    def findById(id: Int): Person
  }

  class PersonDAOImpl extends PersonDAO {
    //添加方法
    override def add(p: Person): Unit = {
      println("Invoking add Method....adding " + p)
    }

    //更新方法
    override def update(p: Person): Unit = {
      println("Invoking update Method,updating " + p)
    }

    //查找方法
    override def findById(id: Int): Person = {
      println("Invoking findById Method,id=" + id)
      Person(1, "John", 18)
    }

    //删除方法
    override def delete(id: Int): Unit = {
      println("Invoking delete Method,id=" + id)
    }
  }

  val p: PersonDAO = new PersonDAOImpl
  p.add(Person(1, "John", 18))
}


/**
  * 带抽象字段的trait
  */
object Example07_02 extends App {

  case class Person(var id: Int, var name: String, var age: Int)

  //带抽象成员变量的trait
  trait PersonDAO {
    //抽象成员变量
    var recordNum: Long

    //添加方法
    def add(p: Person)

    //更新方法
    def update(p: Person)

    //删除方法
    def delete(id: Int)

    //查找方法
    def findById(id: Int): Person
  }

  class PersonDAOImpl extends PersonDAO {
    //抽象成员具体化
    override var recordNum: Long = _

    //添加方法
    override def add(p: Person): Unit = {
      println("Invoking add Method....adding " + p)
    }

    //更新方法
    override def update(p: Person): Unit = {
      println("Invoking update Method,updating " + p)
    }

    //查找方法
    override def findById(id: Int): Person = {
      println("Invoking findById Method,id=" + id)
      Person(1, "John", 18)
    }

    //删除方法
    override def delete(id: Int): Unit = {
      println("Invoking delete Method,id=" + id)
    }


  }

  val p: PersonDAO = new PersonDAOImpl
  p.add(Person(1, "John", 18))
}


/**
  * 带具体成员变量的trait
  */
object Example07_03 extends App {

  case class Person(var id: Int, var name: String, var age: Int)

  //带具体成员变量的trait
  trait PersonDAO {
    //抽象成员
    var recordNum: Long = _

    //添加方法
    def add(p: Person)

    //更新方法
    def update(p: Person)

    //删除方法
    def delete(id: Int)

    //查找方法
    def findById(id: Int): Person
  }

  class PersonDAOImpl extends PersonDAO {
    //添加方法
    override def add(p: Person): Unit = {
      println("Invoking add Method....adding " + p)
    }

    //更新方法
    override def update(p: Person): Unit = {
      println("Invoking update Method,updating " + p)
    }

    //查找方法
    override def findById(id: Int): Person = {
      println("Invoking findById Method,id=" + id)
      Person(1, "John", 18)
    }

    //删除方法
    override def delete(id: Int): Unit = {
      println("Invoking delete Method,id=" + id)
    }


  }

  val p: PersonDAO = new PersonDAOImpl
  p.add(Person(1, "John", 18))
}


/**
  * 带具体成员方法的trait
  */
object Example07_04 extends App {

  case class Person(var id: Int, var name: String, var age: Int)

  //带具体成员方法的trait
  trait PersonDAO {

    //具体成员方法
    def add(p: Person): Unit = {
      println("Invoking add Method....adding " + p)
    }

  }

}


/**
  * trait构造顺序
  */
object Example07_05 extends App {

  import java.io.PrintWriter

  trait Logger {
    println("Logger")

    def log(msg: String): Unit
  }

  trait FileLogger extends Logger {
    println("FileLogger")
    val fileOutput = new PrintWriter("file.log")
    fileOutput.println("#")

    def log(msg: String): Unit = {
      fileOutput.print(msg)
      fileOutput.flush()
    }
  }

  new FileLogger {}.log("trait")

}


/**
  * trait构造顺序
  */
object Example07_06 extends App {

  trait Logger {
    println("Logger")
  }

  trait FileLogger extends Logger {
    println("FileLogger")
  }

  trait Closable {
    println("Closable")
  }

  class Person {
    println("Constructing Person....")
  }

  class Student extends Person with FileLogger with Closable {
    println("Constructing Student....")
  }

  new Student
}


/**
  *
  */
object Example07_07 extends App {

  import java.io.PrintWriter

  trait Logger {
    def log(msg: String): Unit
  }

  trait FileLogger extends Logger {
    //增加了抽象成员变量
    val fileName: String
    //将抽象成员变量作为PrintWriter参数
    val fileOutput = new PrintWriter(fileName: String)
    fileOutput.println("#")

    def log(msg: String): Unit = {
      fileOutput.print(msg)
      fileOutput.flush()
    }
  }

  class Person

  class Student extends Person with FileLogger {
    //Student类对FileLogger中的抽象字段进行重写
    val fileName = "file.log"
  }

  new Student().log("trait demo")
}


/**
  * 提前定义
  */
object Example07_08 extends App {

  import java.io.PrintWriter

  trait Logger {
    def log(msg: String): Unit
  }

  trait FileLogger extends Logger {

    val fileName: String
    val fileOutput = new PrintWriter(fileName: String)
    fileOutput.println("#")

    def log(msg: String): Unit = {
      fileOutput.print(msg)
      fileOutput.flush()
    }
  }

  class Person

  class Student extends Person with FileLogger {
    val fileName = "file.log"
  }


  val s = new {
    //提前定义
    override val fileName = "file.log"
  } with Student
  s.log("predifined variable ")

  val x=9
  if(x<10)
    if(x==9)
      println("9")
    else
      println("other")
  else
    println("bigger than 9")
}


/**
  * 懒加载
  */
object Example07_09 extends App {
  import java.io.PrintWriter

  trait Logger {
    def log(msg: String): Unit
  }

  trait FileLogger extends Logger {

     val fileName: String
    //成员变量定义为lazy
    lazy val fileOutput = new PrintWriter(fileName: String)


    def log(msg: String): Unit = {
      fileOutput.print(msg)
      fileOutput.flush()
    }
  }

  class Person

  class Student extends Person with FileLogger {
    val fileName = "file.log"
  }

  val s = new  Student
  s.log("#")
  s.log("lazy demo")
}


