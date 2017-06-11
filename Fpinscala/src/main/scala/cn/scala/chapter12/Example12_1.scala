package cn.scala.chapter12

import java.util.concurrent.TimeUnit

import akka.actor.TypedActor.{PostStop, PreStart, Receiver}
import akka.actor._
import akka.event.{LoggingAdapter, Logging}
import akka.routing.{RoundRobinRouter, BroadcastRouter}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await


/**
  * 创建Actor：使用默认构造函数创建Actor
  */
object Example12_1 extends  App{
  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging
  import akka.actor.ActorSystem

  //定义自己的Actor，通过extends Actor并实现receive方法进行定义
  class StringActor extends Actor {
    val log = Logging(context.system, this)
    def receive = {
      case s:String ⇒ log.info("received message:\n"+s)
      case _      ⇒ log.info("received unknown message")
    }
  }

  //创建ActorSystem,ActorSystem为创建和查找Actor的入口
  //ActorSystem管理的Actor共享配置信息如分发器(dispatchers)、部署（deployments）等
  val system = ActorSystem("StringSystem")

  //使用默认的构造函数创建Actor实例
  val stringActor = system.actorOf(Props[StringActor])

  //给stringActor发送字符串消息
  stringActor!"Creating Actors with default constructor"

  //关闭ActorSystem
  system.shutdown()
}


/**
  * 创建Actor：使用非默认构造函数创建Actor
  */
object Example12_2 extends  App{
  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging
  import akka.actor.ActorSystem

  //定义自己的Actor，通过extends Actor并实现receive方法进行定义
   class StringActor(var name:String) extends Actor {
    val log = Logging(context.system, this)
    def receive = {
      case s:String ⇒ log.info("received message:\n"+s)
      case _      ⇒ log.info("received unknown message")
    }
  }

  //创建ActorSystem,ActorSystem为创建和查找Actor的入口
  //ActorSystem管理的Actor共享配置信息如分发器(dispatchers)、部署（deployments）等
  val system = ActorSystem("StringSystem")


  val sa=new StringActor("StringActor")
  //使用非默认的构造函数创建Actor实例，注意这里是Props()，而非Props[]
  val stringActor = system.actorOf(Props(sa),name="StringActor")

  //给stringActor发送字符串消息
  stringActor!"Creating Actors with non-default constructor"

  //关闭ActorSystem
  system.shutdown()
}


/**
  * 消息处理：!(Fire-Forget)
  */
object Example12_4 extends  App{
  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging
  import akka.actor.ActorSystem

  case class Start(var msg:String)
  case class Run(var msg:String)
  case class Stop(var msg:String)

  class ExampleActor extends Actor {
    val other = context.actorOf(Props[OtherActor], "OtherActor")
    val log = Logging(context.system, this)
    def receive={
      //使用fire-and-forget消息模型向OtherActor发送消息，隐式地传递sender
      case Start(msg) => other ! msg
      //使用fire-and-forget消息模型向OtherActor发送消息，直接调用tell方法，显式指定sender
      case Run(msg) => other.tell(msg, sender)
    }
  }

  class OtherActor extends  Actor{
    val log = Logging(context.system, this)
    def receive ={
      case s:String=>log.info("received message:\n"+s)
      case _      ⇒ log.info("received unknown message")
    }
  }



  //创建ActorSystem,ActorSystem为创建和查找Actor的入口
  //ActorSystem管理的Actor共享配置信息如分发器(dispatchers)、部署（deployments）等
  val system = ActorSystem("MessageProcessingSystem")


  //创建ContextActor
  val exampleActor = system.actorOf(Props[ExampleActor],name="ExampleActor")

  //使用fire-and-forget消息模型向exampleActor发送消息
  exampleActor!Run("Running")
  exampleActor!Start("Starting")

  //关闭ActorSystem
  system.shutdown()
}



/**
  * 创建Actor：通过隐式变量context创建Actor
  */
object Example12_3 extends  App{
  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging
  import akka.actor.ActorSystem

  //定义自己的Actor，通过extends Actor并实现receive方法进行定义
  class StringActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case s:String ⇒ log.info("received message:\n"+s)
      case _      ⇒ log.info("received unknown message")
    }
  }

  //再定义一个Actor，在内部通过context创建Actor
  class ContextActor extends Actor{
    val log = Logging(context.system, this)
    //通过context创建StringActor
    var stringActor=context.actorOf(Props[StringActor],name="StringActor")
    def receive = {
      case s:String ⇒ log.info("received message:\n"+s);stringActor!s
      case _      ⇒ log.info("received unknown message")
    }
  }


  //创建ActorSystem,ActorSystem为创建和查找Actor的入口
  //ActorSystem管理的Actor共享配置信息如分发器(dispatchers)、部署（deployments）等
  val system = ActorSystem("StringSystem")


  //创建ContextActor
  val stringActor = system.actorOf(Props[ContextActor],name="ContextActor")

  //给stringActor发送字符串消息
  stringActor!"Creating Actors with implicit val context"

  //关闭ActorSystem
  system.shutdown()
}


/**
  * 消息处理：?(Send-And-Receive-Future)
  */
object Example12_5 extends  App{
  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging
  import akka.actor.ActorSystem
  import scala.concurrent.Future
  import akka.pattern.ask
  import akka.util.Timeout
  import scala.concurrent.duration._
  import akka.pattern.pipe
  import scala.concurrent.ExecutionContext.Implicits.global

  //消息：个人基础信息
  case class BasicInfo(id:Int,val name:String, age:Int)
  //消息：个人兴趣信息
  case class InterestInfo(id:Int,val interest:String)
  //消息: 完整个人信息
  case class Person(basicInfo: BasicInfo,interestInfo: InterestInfo)


  //基础信息对应Actor
  class BasicInfoActor extends Actor{
    val log = Logging(context.system, this)
    def receive = {
      //处理送而来的用户ID，然后将结果发送给sender（本例中对应CombineActor）
      case id:Int ⇒log.info("id="+id);sender!new BasicInfo(id,"John",19)
      case _      ⇒ log.info("received unknown message")
    }
  }

  //兴趣爱好对应Actor
  class InterestInfoActor extends Actor{
    val log = Logging(context.system, this)
    def receive = {
      //处理送而来的用户ID，然后将结果发送给sender（本例中对应CombineActor）
      case id:Int ⇒log.info("id="+id);sender!new InterestInfo(id,"足球")
      case _      ⇒ log.info("received unknown message")
    }
  }

  //Person完整信息对应Actor
  class PersonActor extends Actor{
    val log = Logging(context.system, this)
    def receive = {
      case person: Person =>log.info("Person="+person)
      case _      ⇒ log.info("received unknown message")
    }
  }


  class CombineActor extends Actor{
    implicit val timeout = Timeout(5 seconds)
    val basicInfoActor = context.actorOf(Props[BasicInfoActor],name="BasicInfoActor")
    val interestInfoActor = context.actorOf(Props[InterestInfoActor],name="InterestInfoActor")
    val personActor = context.actorOf(Props[PersonActor],name="PersonActor")
    def receive = {
      case id: Int =>
        val combineResult: Future[Person] =
          for {
            //向basicInfoActor发送Send-And-Receive-Future消息，mapTo方法将返回结果映射为BasicInfo类型
            basicInfo <- ask(basicInfoActor, id).mapTo[BasicInfo]
            //向interestInfoActor发送Send-And-Receive-Future消息，mapTo方法将返回结果映射为InterestInfo类型
            interestInfo <- ask(interestInfoActor, id).mapTo[InterestInfo]
          } yield Person(basicInfo, interestInfo)

        //将Future结果发送给PersonActor
       pipe(combineResult).to(personActor)

    }
  }

  val _system = ActorSystem("Send-And-Receive-Future")
  val combineActor = _system.actorOf(Props[CombineActor],name="CombineActor")
  combineActor ! 12345
  Thread.sleep(5000)
  _system.shutdown


}


/*
 * 停止Actor
 */
object Example12_7 extends  App {

  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging
  import akka.actor.ActorSystem

  //定义自己的Actor，通过extends Actor并实现receive方法进行定义
  class StringActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case s:String ⇒ log.info("received message:\n"+s)
      case _      ⇒ log.info("received unknown message")
    }

    override def postStop(): Unit = {
      log.info("postStop in StringActor")
    }
  }

  //再定义一个Actor，在内部通过context创建Actor
  class ContextActor extends Actor{
    val log = Logging(context.system, this)
    //通过context创建StringActor
    var stringActor=context.actorOf(Props[StringActor],name="StringActor")
    def receive = {
      case s:String ⇒ {
        log.info("received message:\n"+s)

        stringActor!s
        //停止StringActor
        context.stop(stringActor)
      }
      case _      ⇒ log.info("received unknown message")
    }

    override def postStop(): Unit =  log.info("postStop in ContextActor")
  }


  //创建ActorSystem,ActorSystem为创建和查找Actor的入口
  //ActorSystem管理的Actor共享配置信息如分发器(dispatchers)、部署（deployments）等
  val system = ActorSystem("StringSystem")


  //创建ContextActor
  val stringActor = system.actorOf(Props[ContextActor],name="ContextActor")

  //给stringActor发送字符串消息
  stringActor!"Creating Actors with implicit val context"

  //关闭ActorSystem
  //system.shutdown()
}

/*
 * Actor其它常用方法
 */
object Example12_6 extends  App {

  class StringActor extends Actor {
    val log = Logging(context.system, this)

    //创建Actor时调用，在接受和处理消息前执行。主要用于Actor的初始化等工作
    override def preStart(): Unit = {
      log.info("preStart method in StringActor")
    }

    //Actor停止时调用的方法
    override def postStop(): Unit = {
      log.info("postStop method in StringActor")
    }

    //有未能处理的消息时调用
    override def unhandled(message: Any): Unit = {
      log.info("unhandled method in StringActor")
      super.unhandled(message)
    }

    def receive = {
      case s:String ⇒ log.info("received message:\n"+s)
    }
  }

  val system = ActorSystem("StringSystem")

  //使用默认的构造函数创建Actor实例
  val stringActor = system.actorOf(Props[StringActor],name="StringActor")

  //给stringActor发送字符串消息
  stringActor!"Creating Actors with default constructor"

  //给StringActor发送整型数据，触发调用unhandled方法
  stringActor!123

  //关闭ActorSystem
  system.shutdown()
}


/*
 *  停止Actor:使用PosionPill
 */
object Example12_8 extends  App {

  import akka.actor.Actor
  import akka.actor.Props
  import akka.event.Logging
  import akka.actor.ActorSystem
  import scala.concurrent.Future
  import scala.concurrent.duration._



  //定义自己的Actor，通过extends Actor并实现receive方法进行定义
  class StringActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case s:String ⇒ log.info("received message:\n"+s)
      case _      ⇒ log.info("received unknown message")
    }

    override def postStop(): Unit = {
      log.info("postStop in StringActor")
    }
  }

  //再定义一个Actor，在内部通过context创建Actor
  class ContextActor extends Actor{
    val log = Logging(context.system, this)
    //通过context创建StringActor
    var stringActor=context.actorOf(Props[StringActor],name="StringActor")
    def receive = {
      case s:String ⇒ {
        log.info("received message:\n"+s)

        stringActor!s
        //停止StringActor
        context.stop(stringActor)
      }
      case _      ⇒ log.info("received unknown message")
    }

    override def postStop(): Unit =  log.info("postStop in ContextActor")
  }


  //创建ActorSystem,ActorSystem为创建和查找Actor的入口
  //ActorSystem管理的Actor共享配置信息如分发器(dispatchers)、部署（deployments）等
  val system = ActorSystem("StringSystem")


  //创建ContextActor
  val contextActor = system.actorOf(Props[ContextActor],name="ContextActor")


  contextActor!"Creating Actors with implicit val context"

  //发送PoisonPill消息，停止Actor
  contextActor!PoisonPill


  import akka.pattern.gracefulStop
  import scala.concurrent.Await
  try {
    val stopped: Future[Boolean] = gracefulStop(contextActor, 5 seconds)(system)
    Await.result(stopped, 6 seconds)

  } catch {
     case e: akka.pattern.AskTimeoutException=>
  }
  //关闭ActorSystem
  //system.shutdown()
}



/*
 * Typed Actor
 */
object Example12_9 extends  App {

  import akka.event.Logging
  import scala.concurrent.{ Promise, Future }
  import akka.actor.{ TypedActor, TypedProps }
  import scala.concurrent.duration._

  trait Squarer {
    //fire-and-forget消息
    def squareDontCare(i: Int): Unit
    //非阻塞send-request-reply消息
    def square(i: Int): Future[Int]
    //阻塞式的send-request-reply消息
    def squareNowPlease(i: Int): Option[Int]
    //阻塞式的send-request-reply消息
    def squareNow(i: Int): Int
  }

  class SquarerImpl(val name: String) extends Squarer {
    def this() = this("SquarerImpl")

    def squareDontCare(i: Int): Unit = i * i
    def square(i: Int): Future[Int] = Promise.successful(i * i).future
    def squareNowPlease(i: Int): Option[Int] = Some(i * i)
    def squareNow(i: Int): Int = i * i
  }

  val system = ActorSystem("TypedActorSystem")
  val log = Logging(system, this.getClass)

  //使用默认构造函数创建Typed Actor
  val mySquarer: Squarer =
    TypedActor(system).typedActorOf(TypedProps[SquarerImpl](),"mySquarer")

  //使用非默认构造函数创建Typed Actor
    val otherSquarer: Squarer =
      TypedActor(system).typedActorOf(TypedProps(classOf[Squarer],
        new SquarerImpl("SquarerImpl")), "otherSquarer")


  //fire-forget消息发送
  mySquarer.squareDontCare(10)

  //send-request-reply消息发送
  val oSquare = mySquarer.squareNowPlease(10)

  log.info("oSquare="+oSquare)

  val iSquare = mySquarer.squareNow(10)
  log.info("iSquare="+iSquare)

  //Request-reply-with-future 消息发送
  val fSquare = mySquarer.square(10)
  val result = Await.result(fSquare, 5 second)

  log.info("fSquare="+result)

  system.shutdown()
}


/*
 * 停止Typed Actor
 */
object Example12_10 extends  App {

  import akka.event.Logging
  import scala.concurrent.{ Promise, Future }
  import akka.actor.{ TypedActor, TypedProps }
  import scala.concurrent.duration._

  trait Squarer {
    //fire-and-forget消息
    def squareDontCare(i: Int): Unit
    //非阻塞send-request-reply消息
    def square(i: Int): Future[Int]
    //阻塞式的send-request-reply消息
    def squareNowPlease(i: Int): Option[Int]
    //阻塞式的send-request-reply消息
    def squareNow(i: Int): Int
  }


  //混入PostStop和PreStart
  class SquarerImpl(val name: String) extends  Squarer with PostStop with PreStart {
    import TypedActor.context
    val log = Logging(context.system,TypedActor.self.getClass())
    def this() = this("SquarerImpl")

    def squareDontCare(i: Int): Unit = i * i
    def square(i: Int): Future[Int] = Promise.successful(i * i).future
    def squareNowPlease(i: Int): Option[Int] = Some(i * i)
    def squareNow(i: Int): Int = i * i

    def postStop(): Unit={
      log.info ("TypedActor Stopped")
    }
    def preStart(): Unit={
      log.info ("TypedActor  Started")
    }
  }


  val system = ActorSystem("TypedActorSystem")
  val log = Logging(system, this.getClass)

  //使用默认构造函数创建Typed Actor
  val mySquarer: Squarer =
    TypedActor(system).typedActorOf(TypedProps[SquarerImpl](),"mySquarer")


  //使用非默认构造函数创建Typed Actor
  val otherSquarer: Squarer =
    TypedActor(system).typedActorOf(TypedProps(classOf[Squarer],
      new SquarerImpl("SquarerImpl")), "otherSquarer")

  //Request-reply-with-future 消息发送
  val fSquare = mySquarer.square(10)
  val result = Await.result(fSquare, 5 second)
  log.info("fSquare="+result)

  //调用poisonPill方法停止Actor运行
  TypedActor(system).poisonPill(otherSquarer)

  //调用stop方法停止Actor运行
  TypedActor(system).stop(mySquarer)

  //system.shutdown()
}

/*
 * Dispatcher:默认Dispatcher
 */
object Example12_11 extends  App {
  import akka.actor.ActorSystem
  import com.typesafe.config.ConfigFactory
  import akka.actor.Props

  class StringActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case s:String ⇒ {
        log.info("received message:\n"+s)
      };
      case _      ⇒ log.info("received unknown message")
    }

    override def postStop(): Unit = {
      log.info("postStop in StringActor")
    }
  }

  //从application.conf配置文件中加载dispatcher配置信息
  val _system = ActorSystem.create("DsipatcherSystem",ConfigFactory.load().getConfig("Akka-Default-Dsipatcher-Example"))

  //创建Actor时通过withDispatcher方法指定自定义的Dispatcher
  val stringActor = _system.actorOf(Props[StringActor].withDispatcher("defaultDispatcher"),name="StringActor")
  val stringActor1 = _system.actorOf(Props[StringActor].withDispatcher("defaultDispatcher"),name="StringActor1")
  stringActor!"Test"
  stringActor1!"StringActor1"

  _system.shutdown()
}

/*
 * Mailbox
 */
object Example12_12 extends  App {

  import akka.dispatch.PriorityGenerator
  import akka.dispatch.UnboundedPriorityMailbox
  import com.typesafe.config.Config

  //自定义Mailbox,扩展自UnboundedPriorityMailbox
  class MyPrioMailbox(settings: ActorSystem.Settings, config: Config)
    extends UnboundedPriorityMailbox(
      // 创建PriorityGenerator，值越低表示优先级越高
      PriorityGenerator {
        // ’highpriority为符号信息，首先处理（高优先级）
        case 'highpriority  =>0
        // ’lowpriority 为符号信息，最后处理（低优先级）
        case 'lowpriority  =>2
        // PoisonPill 停止Actor运行
        case PoisonPill=>3
        // 默认优先级，值介于高优先级和低优先级之间
        case otherwise => 1
      })


  //从application.conf配置文件中加载dispatcher配置信息
  val _system = ActorSystem.create("DsipatcherSystem",ConfigFactory.load().getConfig("MyDispatcherExample"))

  // We create a new Actor that just prints out what it processes
  val a = _system.actorOf(
    Props(new Actor {
      val log: LoggingAdapter = Logging(context.system, this)
      self ! 'lowpriority
      self ! 'lowpriority
      self ! 'highpriority
      self ! 'pigdog
      self ! 'pigdog2
      self ! 'pigdog3
      self ! 'highpriority
      self ! PoisonPill
      def receive = {
        case x => log.info(x.toString)
      }
    }).withDispatcher("balancingDispatcher"),name="UnboundedPriorityMailboxActor")

  _system.shutdown()
}

/*
 * Router
 */
object Example12_13 extends  App {
  import akka.actor.ActorSystem
  import akka.actor.Props
  import akka.routing.RandomRouter

  class IntActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case s: Int ⇒ {
        log.info("received message:" + s)
      }
      case _ ⇒ log.info("received unknown message")
    }
  }
  val _system = ActorSystem("RandomRouterExample")
  val randomRouter = _system.actorOf(Props[IntActor].withRouter(RandomRouter(5)), name = "IntActor")
  1 to 10 foreach {
    i => randomRouter ! i
  }
  _system.shutdown()

}


/*
 * 容错：Supervisor策略，One-For-One strategy
 */
object Example12_14 extends  App {
  import akka.actor.actorRef2Scala
  import akka.actor.Actor
  import akka.actor.ActorLogging
  import akka.actor.Props
  import scala.concurrent.duration._
  import akka.pattern.ask

  case class NormalMessage()
  class ChildActor extends Actor with ActorLogging {
    var state: Int = 0

    override def preStart() {
      log.info("启动 ChildActor，其hashcode为"+this.hashCode())
    }
    override def postStop() {
      log.info("停止 ChildActor，其hashcode为"+this.hashCode())
    }
    def receive: Receive = {
      case value: Int =>
        if (value <= 0)
          throw new ArithmeticException("数字小于等于0")
        else
          state = value
      case result: NormalMessage =>
        sender ! state
      case ex: NullPointerException =>
        throw new NullPointerException("空指针")
      case _ =>
        throw new IllegalArgumentException("非法参数")
    }
  }

  class SupervisorActor extends Actor with ActorLogging {
    import akka.actor.OneForOneStrategy
    import akka.actor.SupervisorStrategy._
    val childActor = context.actorOf(Props[ChildActor], name = "ChildActor")

    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 seconds) {

      //异常类型为ArithmeticException时，采用Resume机制
      case _: ArithmeticException => Resume
      //异常类型为NullPointerException时，采用Resume机制
      case _: NullPointerException => Restart
      //异常类型为IllegalArgumentException时，采用Stop机制
      case _: IllegalArgumentException => Stop
      //其它异常机制，采用Escalate机制
      case _: Exception => Escalate
    }

    def receive = {
      case msg: NormalMessage =>
        childActor.tell(msg, sender)
      case msg: Object =>
        childActor ! msg

    }
  }

  val system = ActorSystem("FaultToleranceSystem")
  val log = system.log

  val supervisor = system.actorOf(Props[SupervisorActor], name = "SupervisorActor")

  //正数，消息正常处理
  var mesg: Int = 5
  supervisor ! mesg

  implicit val timeout = Timeout(5 seconds)
  var future = (supervisor ? new NormalMessage).mapTo[Int]
  var resultMsg = Await.result(future, timeout.duration)

  log.info("结果:"+resultMsg)

  //负数，Actor会抛出异常，Superrvisor使用Resume处理机制
  mesg = -5
  supervisor ! mesg


  future = (supervisor ? new NormalMessage).mapTo[Int]
  resultMsg = Await.result(future, timeout.duration)

  log.info("结果:"+resultMsg)


  //空指针消息，Actor会抛出异常，Superrvisor使用restart处理机制
  supervisor ! new NullPointerException

  future = (supervisor ? new NormalMessage).mapTo[Int]
  resultMsg = Await.result(future, timeout.duration)

  log.info("结果:"+resultMsg)

  //String类型参数为非法参数，Actor会抛出异常，Superrvisor使用stop处理机制
  supervisor ? "字符串"

  system.shutdown
}


/*
 * 容错：Supervisor策略，All-For-One strategy
 */
object Example12_15 extends  App {
  import akka.actor.actorRef2Scala
  import akka.actor.Actor
  import akka.actor.ActorLogging
  import akka.actor.Props
  import scala.concurrent.duration._
  import akka.pattern.ask

  case class Result()
  class WorkerActor extends Actor with ActorLogging {
    var state: Int = 0

    override def preStart() {
      log.info("Starting WorkerActor instance hashcode # {}", this.hashCode())
    }
    override def postStop() {
      log.info("Stopping WorkerActor instance hashcode # {}", this.hashCode())
    }
    def receive: Receive = {
      case value: Int =>
        if (value <= 0)
          throw new ArithmeticException("Number equal or less than zero")
        else
          state = value
      case result: Result =>
        sender ! state
      case ex: NullPointerException =>
        throw new NullPointerException("Null Value Passed")
      case _ =>
        throw new IllegalArgumentException("Wrong Arguement")
    }
  }

  class AnotherWorkerActor extends Actor with ActorLogging {
    var state: Int = 0

    override def preStart() {
      log.info("Starting AnotherWorkerActor instance hashcode # {}", this.hashCode())
    }
    override def postStop() {
      log.info("Stopping AnotherWorkerActor instance hashcode # {}", this.hashCode())
    }
    def receive: Receive = {
      case value: Int =>
        if (value <= 0)
          throw new ArithmeticException("Number equal or less than zero")
        else
          state = value
      case result: Result =>
        sender ! state
      case ex: NullPointerException =>
        throw new NullPointerException("Null Value Passed")
      case _ =>
        throw new IllegalArgumentException("Wrong Arguement")
    }
  }

  class SupervisorActor extends Actor with ActorLogging {
    import akka.actor.OneForOneStrategy
    import akka.actor.SupervisorStrategy._

    val workerActor1 = context.actorOf(Props[WorkerActor], name = "workerActor1")
    val workerActor2 = context.actorOf(Props[AnotherWorkerActor], name = "workerActor2")

    override val supervisorStrategy = AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 seconds) {

      case _: ArithmeticException => Resume
      case _: NullPointerException => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception => Escalate
    }

    def receive = {
      case result: Result =>
        workerActor1.tell(result, sender)
      case msg: Object =>
        workerActor1 ! msg

    }
  }

  val system = ActorSystem("faultTolerance")
  val log = system.log
  val originalValue: Int = 0

  val supervisor = system.actorOf(Props[SupervisorActor], name = "supervisor")

  log.info("Sending value 8, no exceptions should be thrown! ")
  var mesg: Int = 8
  supervisor ! mesg

  implicit val timeout = Timeout(5 seconds)
  var future = (supervisor ? new Result).mapTo[Int]
  var result = Await.result(future, timeout.duration)

  log.info("Value Received-> {}", result)

  log.info("Sending value -8, ArithmeticException should be thrown! Our Supervisor strategy says resume !")
  mesg = -8
  supervisor ! mesg

  future = (supervisor ? new Result).mapTo[Int]
  result = Await.result(future, timeout.duration)

  log.info("Value Received-> {}", result)

  log.info("Sending value null, NullPointerException should be thrown! Our Supervisor strategy says restart !")
  supervisor ! new NullPointerException

  future = (supervisor ? new Result).mapTo[Int]
  result = Await.result(future, timeout.duration)

  log.info("Value Received-> {}", result)

  log.info("Sending value \"String\", IllegalArgumentException should be thrown! Our Supervisor strategy says Stop !")

  supervisor ? "Do Something"

  system.shutdown
}


//Scheduler
object Example12_30 extends  App {
  import akka.event.Logging
  import scala.concurrent.duration._

  val system = ActorSystem("StringSystem")

  val Tick = "tick"
  val tickActor = system.actorOf(Props(new Actor {
    def receive = {
      case Tick=> println("Tick---Tick")
    }
  }))
  //Use system’s dispatcher as ExecutionContext
  import system.dispatcher
  //This will schedule to send the Tick-message
  //to the tickActor after 0ms repeating every 50ms
  val cancellable =
    system.scheduler.schedule(0 milliseconds,
      50 milliseconds,
      tickActor,
      Tick)
  //This cancels further Ticks to be sent
  //cancellable.cancel()

  //system.shutdown()
}


//Future
object Example12_31 extends  App {
  import scala.concurrent.Await
  import scala.concurrent.Future
  import scala.concurrent.duration._

  val system = ActorSystem("StringSystem")
  import system.dispatcher
//  val future = Future {
//    "Hello" + "World"
//  }
//  future foreach println
//
//  val future1 = Future.successful("Yay!")
//  println(Await.result(future1,5 seconds))
//
//  val otherFuture = Future.failed[String](new IllegalArgumentException("Bang!"))
//  println(Await.result(otherFuture,5 seconds))


  val f1 = Future {
    "Hello" + "World"
  }
  val f2 = f1 map { x =>
    x.length
  }
  f2 foreach println

  system.shutdown()
}
