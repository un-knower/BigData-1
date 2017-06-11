package cn.scala.chapter02

/**
  * Created by 周志湖 on 2016/3/15.
  */
object whileLoop extends  App{
//    var i=15
//    while(i<20){
//      println("i="+i)
//      i=i+1
//    }
//
//    var j=15
//    do{
//      println("i="+i)
//      i=i+1
//    }while(i<20)
//
//
//
//
//  //引入Breaks类及所有的方法
//  import scala.util.control.Breaks._
//  //调用Breaks中定义的breakable方法
//  breakable{
//    for(i<- 1 to 5) {
//      //break为Breaks中定义的方法
//      if(i==3) break
//      println("i="+i)
//    }
//  }

//      for(i<- 1 to 5 if(i<3)) {
//        println("i="+i)
//      }

//      for(i<- 1 to 40 if(i%4==0);if(i%5==0)){
//        println("i="+i)
//      }

//        for(i<- 1 to 5 if(i>3)){
//          for(j<- 5 to 7 if(j==6)){
//            println("i="+i+",j="+j)
//          }
//        }
//
//       var x=for (i <- 1 to 5) yield i

      var i=15
      var x=while(i<20){
        println("i="+i)
        i=i+1
      }

  val set=Set(1,2)
  var p:Symbol=_

  class Person
  def trans() = {
    try {
      (1,2,3)
    }
    catch {
      case x:Exception => new Person
    }
  }

}
