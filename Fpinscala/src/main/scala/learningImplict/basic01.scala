package learningImplict

/**
  * Created by hushiwei on 2017/9/5.
  * desc : 
  */

class LineNumber(val num:Int){
  def + (that:LineNumber)=new LineNumber(this.num+that.num)
}

object basic01 extends App{
  val lineNumberOfPage1=new LineNumber(112)
  val lineNumberOfPage2=new LineNumber(120)

  implicit def intToNumberPage(i:Int)=new LineNumber(i)
  implicit def NumberPageToInt(num:LineNumber)=num.num

  val result=  10+lineNumberOfPage1
  println(result)
}