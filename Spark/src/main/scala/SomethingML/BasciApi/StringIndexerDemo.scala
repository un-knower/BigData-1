package SomethingML.BasciApi

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2018/1/10.
  * desc : 
  */
object StringIndexerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("learn StringIndexed").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    df1.show()

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed1 = indexer.fit(df1).transform(df1)
    indexed1.show()

    println("~"*50)

    val df2 = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "d"))
    ).toDF("id", "category")

    //
    val indexed2 = indexer.fit(df1).setHandleInvalid("skip").transform(df2)

    indexed2.show()
    println("~"*50)
    val indexed3=indexer.fit(df1).transform(df2)
    indexed3.show()
  }

}
