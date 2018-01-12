package SomethingML.Naivebayes

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hushiwei on 2018/1/12.
  * desc :
  * Spark Mlib中的一种数据结构 LabeledPoint
  * LabelPoint 组成结构
  *   label:Double类型   ---> 类别
  *   features:Vector[Double]集合  --->特征集合
  *
  * 数据描述
  *
  * 性别（0女1男）,身高（英尺）	体重（磅）	脚掌（英寸）
  */
object HumanSexPredict {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NaiveBayesExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawtxt=sc.textFile("Spark/src/main/resources/ml/human-body-features.txt")

    // 将文本文件的内容转化为我们需要的数据结构 LabeledPoint
    val allData=rawtxt.map{line=>
      val colData=line.split(",")
      LabeledPoint(colData(0).toDouble,Vectors.dense(colData(1).split(" ").map(_.toDouble)))
    }

    // 训练
    val nbTrained=NaiveBayes.train(allData)

    // 待分类的特征集合
    val txt="6 190 12"
    val vec=Vectors.dense(txt.split(" ").map(_.toDouble))

    // 预测(分类)
    val nbPredict=nbTrained.predict(vec)

    println("预测此人性别是: "+(if(nbPredict==0) "女" else "男"))
  }

}
