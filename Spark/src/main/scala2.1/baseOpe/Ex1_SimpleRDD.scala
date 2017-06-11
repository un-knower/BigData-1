package baseOpe

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object Ex1_SimpleRDD {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleRDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    /**
      * 1.构造一些数据
      * 2.用map算子进行一些操作
      * 3.用mapPartitions算子进行操作
      * 4.调用zipwithpartition算子标记分区
      * 5.调用foreachPartiton进行输出
      * 6.调用glom算子收集每个分区的数据
      * 7.调用flodleft函数处理Array
      */
    def printRDD(rdd:RDD[_])={
      val str=rdd.collect().mkString(" , ")
      println(str)
    }

    val arr=ListBuffer[Int]()
    for (i <- 0 until 10){
      arr +=Random.nextInt(100)
    }

    val numberRDD=sc.parallelize(arr.toList,2)
    println("查看原始的RDD中的每一个元素:")
//    numberRDD.foreach(println)
//    printRDD(numberRDD)
    val numberRDD10=numberRDD.map(_*10)
    printRDD(numberRDD10)
    val numberRDDLess=numberRDD10.mapPartitions(iter=> {
      val arr=new ArrayBuffer[Double]
      while (iter.hasNext) {
        val value=iter.next().toDouble/10
          arr+=value
      }
      arr.iterator
    })
    printRDD(numberRDDLess)
    println("进行shuffle操作")

    // glom 是把每个分区里面的元素放到一个list里面
    val partitions=numberRDDLess.glom()
    println("构造数据的时候创建了 "+numberRDDLess.partitions.size+" 个分区")
    println("目前经过glom操作后,还有: "+partitions.count()+" 个分区,只是每个分区的数据存到到一个list中去了")
    partitions.foreach(arr=>{
      println("分区数据:"+arr.mkString(" , "))
    })

    val numbers=partitions.zipWithIndex()
    numbers.foreach{
      case (arr,v)=>
        println("第 "+ v+"个分区数据: "+arr.mkString(" , "))
    }

    partitions.foreach(arr=>{
      println("分区内容: "+
      arr.foldLeft("")((x,y)=>x+" "+y))
    })

  }
}
