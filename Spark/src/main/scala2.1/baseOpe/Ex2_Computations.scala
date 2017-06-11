package baseOpe

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ex2_Computations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Computations").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 构造一个数据集
    val numbers = sc.parallelize(1 to 10, 4)
    val bigger = numbers.map(n => n * 100)
    val biggerStill = bigger.map(n => n + 1)

    println("调用 toDebugString 算子去查看经过几次转换后,依赖关系是什么样:")
    println(biggerStill.toDebugString)

    //    进行一次reduce操作
    val s = biggerStill.reduce(_ + _)

    println("sum = " + s)

    println("numbersRDD的id = " + numbers.id)
    println("biggerRDD的id = " + bigger.id)
    println("biggerStillRDD的id = " + biggerStill.id)
    println("查看biggerStill RDD 依赖继承关系: ")
    showDependency(biggerStill)

    val moreNumbers = bigger ++ biggerStill
    println("moreNumbers的依赖继承关系: ")
    println(moreNumbers.toDebugString)
    println("moreNumbers: id=" + moreNumbers.id)
    showDependency(moreNumbers)

    moreNumbers.cache()
    // cache操作可能会丢失数据,而且并没有发生依赖的变化
    println("cached moreNumbers的依赖继承关系(并没有变化): ")
    println(moreNumbers.toDebugString)
    println("执行 cache 操作后,moreNumbers的依赖继承关系: ")

    showDependency(moreNumbers)

    println("检查一下moreNumbers有没有设置检查点? : " + moreNumbers.isCheckpointed)
    sc.setCheckpointDir("/tmp/sparkcps")
    moreNumbers.checkpoint()
    println("现在执行了checkpoint 检查一下moreNumbers有没有设置检查点? : " + moreNumbers.isCheckpointed)
    moreNumbers.count()
    println("现在执行了一个count操作 检查一下moreNumbers有没有设置检查点? : " + moreNumbers.isCheckpointed)
    println(moreNumbers.toDebugString)
    println("做了以上操作后,moreNumbers的依赖继承关系: ")
    showDependency(moreNumbers)

    println("这里不应该抛异常...")
    println("因为spark是懒加载,只有遇到action算子的时候,才会开始生成job开始调度计算....")
    val thisWillBlowUp = numbers map {
      case (7) => {
        throw new Exception
      }
      case (n) => n
    }

    println("异常应该在这里抛出来...")
    try {
      println(thisWillBlowUp.count())
    } catch {
      case (e: Exception) => println("Nice,果真在这里抛异常了...")
    }

  }

  // 利用递归函数来输出rdd的依赖继承关系
  def showDependency[T](rdd: RDD[T]): Unit = {
    showDependency(rdd, 0)
  }

  private def showDependency[T](rdd: RDD[T], length: Int): Unit = {
    println("".padTo(length, ' ') + "RDD id= " + rdd.id)
    rdd.dependencies.foreach(dep => {
      showDependency(dep.rdd, length + 1)
    })
  }
}
