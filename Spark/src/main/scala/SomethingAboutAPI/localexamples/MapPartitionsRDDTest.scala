package SomethingAboutAPI.localexamples

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsRDDTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapPartitionsRDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = Array[(String, Int)](("A1", 1), ("A2", 2),
      ("B1", 1), ("B2", 4),
      ("C1", 3), ("C2", 4)
    )
    val pairs = sc.parallelize(data, 3)

    val finalRDD = pairs.mapPartitions(iter => iter.filter(_._2 >= 2))

    val map=Map("s"->1)


    finalRDD.foreachPartition(iter => {
      while (iter.hasNext) {
        val next = iter.next()
        println(next._1 + " --- " + next._2)

      }
    })

  }
}