package cn.scala.chapter09

/**
  * 使用Scala中的模式匹配可以避免Java语言中switch语句会意外陷入分支的情况
  *
  */
object ScalaExampel9_1 extends  App{
for(i<- 1 to 5)
  //Scala模式匹配
  i match {
    //仅匹配值为1的情况，不会意外陷入分支
    case 1=> println(1)
    case 2=> println(2)
    case 3=> println(3)
    //_通配符表示，匹配其它情况，与Java switch语句中的default相同
    case _=>println("其它")
  }
}

/**
  * Scala模式匹配中的case语句还可以加入表达式
  *
  */
object ScalaExampel9_2 extends  App{
  for(i<- 1 to 6)
  //Scala模式匹配
    i match {
      case 1=> println(1)
      //case 语句中可以加入表达式
      case x if (x%2==0)=>println(s"$x 能够被2整除")
      //其它情况则不进行任何操作
      case _=>
    }
}

/**
  * 函数中使用模式匹配，模式匹配结果作为函数返回值
  *
  */
object ScalaExampel9_3 extends  App{
  //函数中使用模式匹配，匹配结果作为函数的返回值
  def patternMatching(x:Int)=x match {
    case 5 => "整数5"
    case x if(x%2==0) =>"能被2整除的数"
    case _ =>"其它整数"
  }
  println(patternMatching(5))
  println(patternMatching(4))
  println(patternMatching(3))
}

/**
  * 变量模式
  */
object ScalaExampel9_4 extends  App{

  def patternMatching(x:Int)=x match {
    case i if(i%2==0) =>"能被2整除的数"
    case i if(i%3==0) =>"能被3整除的数"
    case i => "除能被2或3整除外的其它整数"
  }
  println(patternMatching(5))
  println(patternMatching(4))
  println(patternMatching(6))
}

/**
  * 注意变量模式匹配顺序
  *  case i =>这种变量匹配模式的作用等同case _
  */
object ScalaExampel9_5 extends  App{

  def patternMatching(x:Int)=x match {
    //case i变量模式等同于case _
    case i => "匹配任何整数，后面两条变量模式不会执行"
    case i if(i%2==0) =>"能被2整除的数"
    case i if(i%3==0) =>"能被3整除的数"
  }
  println(patternMatching(5))
  println(patternMatching(4))
  println(patternMatching(3))
}
/**
  * 构造器模式
  */
object ScalaExampel9_6 extends  App{

  //定义一个case class
  case class Dog(val name:String,val age:Int)
  //利用构造器创建对象
  val dog=Dog("Pet",2)

  def patternMatching(x:AnyRef)=x match {
    //构造器模式,其作用与相反，用于对象进行解构（也称析构）
    case Dog(name,age) => println(s"Dog name=$name,age=$age")
    case _=>
  }
  patternMatching(dog)
}

/**
  * 构造器，构取对象的部分成员域
  */
object ScalaExampel9_7 extends  App{
  //定义一个case class
  case class Dog(val name:String,val age:Int)
  //利用构造器创建对象
  val dog=Dog("Pet",2)

  def patternMatching(x:AnyRef)=x match {
    //构造器模式,通配符匹配对象成员域name，但程序中不需要该成员域的值
    case Dog(_,age) => println(s"Dog age=$age")
    case _=>
  }
  patternMatching(dog)
}

/**
  * 序列模式
  */
object ScalaExampel9_8 extends  App{
  val arrInt=Array(1,2,3,4)
  def patternMatching(x:AnyRef)=x match {
    //序列模式，与构建序列的作用相反，用于对序列中的元素内容进行析取
    case Array(first,second) => println(s"序列中第一个元素=$first,第二个元素=$second")
    //first,second分别匹配序列中的第一、二个元素，_*匹配序列中剩余其它元素
    case Array(first,_,three,_*) => println(s"序列中第一个元素=$first,第三个元素=$three")
    case _=>
  }
  patternMatching(arrInt)
}

/**
  * 元组模式
  */
object ScalaExampel9_9 extends  App{
  //定义一个元组
  val tupleInt=(1,2,3,4)
  def patternMatching(x:AnyRef)=x match {
    //元组模式，匹配两个元素的元组
    case (first,second) => println(s"元组中第一个元素=$first,第二个元素=$second")

    //first,three分别匹配元组中的第一、三个元素
    //第一个_匹配元组中的第二个元素，第二个_匹配元组中的第四个元素
    case (first,_,three,_) => println(s"元组中第一个元素=$first,第三个元素=$three")

    //元组模式中不能使用_*的匹配模式
    //case (first,_*) => println(s"元组中第一个元素=$first")
    case _=>
  }
  patternMatching(tupleInt)
}

/**
  * 类型模式
  */
object ScalaExampel9_10 extends  App{
  //定义一个元组
  class A
  class B extends A
  class C extends A
  val b=new B
  val c=new C
  def patternMatching(x:AnyRef)=x match {
      //类型模式，匹配字符串
    case x:String=>println("字符串类型")
      //类型模式，匹配类B
    case x:B=>println("对象类型为B")
      //类型模式，匹配类A
    case x:A=>println("对象类型为A")
    case _=>println("其它类型")
  }
  patternMatching("Scala Pattern Matching")
  patternMatching(b)
  patternMatching(c)
}

/**
  * 变量绑定模式
  */
object ScalaExampel9_11 extends  App{
  //定义一个元组
  case class Dog(val name:String,val age:Int)
  val dog=Dog("Pet",2)

  def patternMatching(x:AnyRef)=x match {
    //变量绑定模式，匹配成功，则将整个对象赋值给变量 d
    case d@Dog(_,_)=>println("变量绑定模式返回的变量值为："+d)
    case _=>
  }
  patternMatching(dog)
}

/**
  * 变量绑定模式
  */
object ScalaExampel9_12 extends  App{
  //定义一个元组
  val list=List(List(1,2,3,4),List(4,5,6,7,8,9))
  //println(list)
  def patternMatching(x:AnyRef)=x match {
    //变量绑定模式
    case e1@List(_,e2@List(4,_*))=>println("变量e1="+e1+"\n变量e2="+e2)
    case _=>
  }
  patternMatching(list)
}


/**
  * 构造器模式匹配原理：手动在类的伴生对象中实现unapply方法
  */
object ScalaExampel9_13 extends  App{

  //定义普通的类Dog
  class Dog(val name:String,val age:Int)
  //Dog类的伴生对象
  object Dog{
    //unapply方法
    def unapply(dog:Dog):Option[(String,Int)]={
            if(dog!=null) Some(dog.name,dog.age)
            else None
    }
  }
  //利用构造器创建对象,这里必须通过显示地new来创建对象
  val dog=new Dog("Pet",2)

  def patternMatching(x:AnyRef)=x match {
    //因为在Dog的伴生对象中实现了unapply方法，此处可以使用构造器模式
    case Dog(name,age) => println(s"Dog name=$name,age=$age")
    case _=>
  }
  patternMatching(dog)
}

/**
  * Scala调用Java语言提供的API进行正则表达式处理
  */
object ScalaExampel9_14 extends  App{
     import java.util.regex.Pattern
      //正则表达式待匹配的字符串
      val line = "Hadoop has been the most popular big data " +
                 "processing tool since 2005-11-21"
      //正则表达式，用于匹配年-月-日这样的日期格式
      val rgex = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)"

      // 根据正则表达式创建 Pattern 对象
      val pattern = Pattern.compile(rgex)

      // 创建 Matcher 对象
      val m = pattern.matcher(line);
      if (m.find( )) {
        //m.group(0)返回整个匹配结果，即(\d\d\d\d)-(\d\d)-(\d\d)匹配结果
        println(m.group(0))
        //m.group(1)返回第一个分组匹配结果，即(\d\d\d\d)年匹配结果
        println(m.group(1))
        //m.group(2)返回第二个分组匹配结果，即(\d\d)月匹配结果
        println(m.group(2))
        //m.group(3)返回第三个分组匹配结果，即(\d\d)日匹配结果
        println(m.group(3))
      } else {
         println("未找到匹配项")
      }
}

/**
  * Scala正则表达式使用
  */
object ScalaExampel9_15 extends  App{

  // 待查找字符串
  val line = "Hadoop has been the most popular big data " +
    "processing tool over the 11 years"
  //正则表达式
  val rgex ="""(\d\d\d\d)-(\d\d)-(\d\d)""".r

//  for (date <- rgex findAllMatchIn "2015-12-31 2016-02-20") {
//    println(date.groupCount)
//  }
//
//  val copyright: String = rgex findFirstIn "Date of this document: 2011-07-15" match {
//    case Some(date)=> "Copyright "+date
//    case None                           => "No copyright"
//  }

  //创建正则表达式对象时，指定模式的分组名称
  val dateP2 = new scala.util.matching.Regex("""(\d\d\d\d)-(\d\d)-(\d\d)""", "year", "month", "day")
  dateP2 findFirstMatchIn "2015-12-31 2016-02-20" match {
    //m.group方法可以返回对应匹配分组的值
    case Some(m)=> println("year对应分组的值为： "+m.group("year"))
    case None                           => "未匹配成功"
  }

  import scala.util.matching.Regex
  val datePattern = new Regex("""(\d\d\d\d)-(\d\d)-(\d\d)""", "year", "month", "day")
  val text = "From 2011-07-15 to 2011-07-17"
  val repl = datePattern replaceAllIn (text, m => m.group("month")+"/"+m.group("day"))
}

/**
  * Scala正则表达式使用
  * 提取模式的分组值
  */
object ScalaExampel9_16 extends  App{
  //正则表达式
  val dateRegx ="""(\d\d\d\d)-(\d\d)-(\d\d)""".r

  //待匹配的字符串
  val text="2015-12-31 2016-02-20"

  //提取模式的分组信息
  for (date<- dateRegx.findAllIn(text)) {
    date match {
      case dateRegx(year,month,day)=>println(s"match语句中的模式匹配：year=$year,month=$month,day=$day")
      case _=>
    }
  }

  //提取模式的分组信息，与前面的代码作用等同
  for (dateRegx(year,month,day)<- dateRegx.findAllIn(text)) {
     println(s"for循环中的正则表达式模式匹配：year=$year,month=$month,day=$day")
  }


}

  /**
    * Scala正则表达式使用
    * findFirstMatchIn等方法与模式匹配
    */
  object ScalaExampel9_17 extends  App{
    //正则表达式
    val dateRegx ="""(\d\d\d\d)-(\d\d)-(\d\d)""".r

    //待匹配的字符串
    val text="2015-12-31 2016-02-20"

    //findFirstMatchIn返回值类型为Option[Match]
    dateRegx.findFirstMatchIn(text) match{
      case Some(dateRegx(year,month,day))=>println(s"findFirstMatchIn与模式匹配：year=$year,month=$month,day=$day")
      case None=>println("没有找到匹配")
    }

    //findFirstIn返回值类型为Option[String]
    dateRegx.findFirstIn(text) match{
      case Some(dateRegx(year,month,day))=>println(s"findFirstIn与模式匹配：year=$year,month=$month,day=$day")
      case None=>println("没有找到匹配")
    }

  }


/**
  * For循环模式匹配
    */
object ScalaExampel9_18 extends  App{

  //使用for循环，给变量进行赋值
  for(i<- 1 to 5){
     print(i+" ")
  }
  println

  //使用for循环，模式匹配的视角（变量模式匹配）
  for((language,framework)<-Map("Java"->"Hadoop","Closure"->"Storm","Scala"->"Spark")){
    println(s"$framework is developed by $language language")
  }

  //使用for循环，模式匹配的视角，（常量模式匹配）
  // 只会输出Spark is developed by Scala language
  for((language,"Spark")<-Map("Java"->"Hadoop","Closure"->"Storm","Scala"->"Spark")){
    println(s"Spark is developed by $language language")
  }

  //使用for循环，模式匹配的视角，（变量绑定模式匹配）
  //只会输出Spark is developed by Scala language
  for((language,e@"Spark")<-Map("Java"->"Hadoop","Closure"->"Storm","Scala"->"Spark")){
    println(s"$e is developed by $language language")
  }

  //使用for循环，模式匹配的视角，（类型模式匹配）
  //只会输出Spark is developed by Scala language
  for((language,framework:String)<-Map("Java"->"Hadoop".length,"Closure"->"Storm".length,"Scala"->"Spark")){
    println(s"$language is developed by $language language")
  }

  //使用for循环，模式匹配的视角，（构造器模式匹配）
  case class Dog(val name:String,val age:Int)
  for(Dog(name,age)<-List(Dog("Pet",2),Dog("Penny",3),Dog("Digo",2))){
    println(s"Dog $name is  $age years old")
  }

  //使用for循环，模式匹配的视角，（序列模式匹配）
  for(List(first,_*)<-List(List(1,2,3),List(4,5,6,7))){
     println(s"the first elemement is $first")
  }



}



/**
  * 模式匹配与样例类
  */
object ScalaExampel9_19 extends  App{
  //定义一个sealed trait DeployMessage
  sealed trait DeployMessage

  //定义三个具体的子类，全部为case class
  case class RegisterWorker(id: String, host: String, port: Int) extends DeployMessage
  case class UnRegisterWorker(id: String, host: String, port: Int) extends DeployMessage
  case class Heartbeat(workerId: String) extends DeployMessage
  //handleMessage函数会处理所有可能的情况，即穷举出所有DeployMessage的子类
  def handleMessage(msg:DeployMessage)= msg match {
    case RegisterWorker(id,host,port)=>s"The worker $id is registering on $host:$port"
    case UnRegisterWorker(id,host,port)=>s"The worker $id is unregistering on $host:$port"
    //case Heartbeat(id)=>s"The worker $id is sending heartbeat"
  }

  val msgRegister=RegisterWorker("204799","192.168.1.109",8079)
//  val msgUnregister=UnRegisterWorker("204799","192.168.1.109",8079)
//  val msgHeartbeat=Heartbeat("204799")

  println(handleMessage(msgRegister))

}


/**
  * 模式匹配与样例对象
  */
object ScalaExampel9_20 extends  App{
  //定义一个sealed trait DeployMessage
  sealed trait DeployMessage

  //定义三个具体的子类，全部为case class
  case class RegisterWorker(id: String, host: String, port: Int) extends DeployMessage
  case class UnRegisterWorker(id: String, host: String, port: Int) extends DeployMessage
  case class Heartbeat(workerId: String) extends DeployMessage
  case  object RequestWorkerState extends  DeployMessage
  //handleMessage函数会处理所有可能的情况，即穷举出所有DeployMessage的子类
  def handleMessage(msg:DeployMessage)= msg match {
    case RegisterWorker(id,host,port)=>s"The worker $id is registering on $host:$port"
    case UnRegisterWorker(id,host,port)=>s"The worker $id is unregistering on $host:$port"
    case Heartbeat(id)=>s"The worker $id is sending heartbeat"
    case RequestWorkerState=>"Request Worker State"
  }

  val msgRegister=RegisterWorker("204799","192.168.1.109",8079)
  //  val msgUnregister=UnRegisterWorker("204799","192.168.1.109",8079)
  //  val msgHeartbeat=Heartbeat("204799")

  println(handleMessage(msgRegister))
  println(handleMessage(RequestWorkerState))

}


