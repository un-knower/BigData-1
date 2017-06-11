package SparkSourceCodeLearning.Dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, ShuffleDependency, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{HashSet, Stack}

/**
  * Created by HuShiwei on 2016/11/3 0003.
  */
object getShuffleMapStage {
  def main(args: Array[String]) {
    val conf= new SparkConf().setAppName("dependency").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val letterRDD=sc.parallelize('a' to 'z')
    val numberRDD=sc.parallelize(1 to 26)
    val numberRDDPlus=numberRDD.map(_+10)
    val shuffRDD=numberRDDPlus.repartition(4)
    val allRDD=letterRDD.zip(numberRDDPlus)

//      showRDDDependency(allRDD)
//    getRDDDependency(shuffRDD)
    allRDD.dependencies.foreach(dep=>{
      println(dep.rdd)
    })

  }

  def showRDDDependency(rdd:RDD[_]): Unit ={

    rdd.dependencies.foreach(showDependency(_))

    def showDependency(dep:Dependency[_]):Unit={
      println("dependency type : "+dep.getClass)
      println("dependency rdd : "+dep.rdd)
      println("dependency partitions : "+dep.rdd.partitions)
      println("dependency partitions size : "+dep.rdd.partitions.length)
      println(" ---> dep完成。。。")

    }
  }


  def getRDDDependency(rdd:RDD[_]):Unit={
    println("--- get denpendency start ---")

    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]

    val waitingForVisit=new mutable.Stack[RDD[_]]()
    def visit(rdd:RDD[_]):Unit= {
      if (!visited(rdd)) {
        visited+=rdd
        for (dep<- rdd.dependencies) {
          println(dep)
          dep match {
            case shufDep:ShuffleDependency[_,_,_]=>
            case _=>
              waitingForVisit.push(dep.rdd)
          }
        }

      }
    }
    waitingForVisit.push(rdd)

    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }

  }

}
