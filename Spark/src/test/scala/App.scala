/**
  * Created by HuShiwei on 2016/8/11 0011.
  */
object App {
  def main(args: Array[String]) {
    def mergesort[T](less: (T,T) => Boolean) (input: List[T]): List[T] = {
      /*
       * @param xList 要合并的有序集合
       * @param yList 要合并的有序集合
       * @return 合并后的集合
       */
      def merge(xList: List[T], yList: List[T]): List[T] = {
        (xList, yList) match {
          case(Nil,_) => yList
          case(_,Nil) => xList
          case(x :: xtail, y :: ytail) =>
            if (less(x,y)) x :: merge(xtail, yList)
            else y :: merge(xList, ytail)
        }
      }

      val n = input.length / 2 // 将输入集合拆分为两个子集合
      if (n ==0) input
      else {
        val (x,y) = input splitAt n
        merge(mergesort(less)(x),mergesort(less)(y))
      }
    }

    println(mergesort((x: Int, y: Int) => x < y)(List(2,28,1,35,22)))
    val reversed_mergedsort=mergesort((x: Int, y: Int) => x > y) _
    println(reversed_mergedsort(List(2,28,1,35,22)))



  }

}
