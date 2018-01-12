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
  * label:Double类型   ---> 类别
  * features:Vector[Double]集合  --->特征集合
  *
  * 数据描述
  *
  * 性别（0女1男）,身高（英尺）	体重（磅）	脚掌（英寸）
  *
  * 分类器的质量评价
  * 在构造初期,将训练数据一分为二(trainDataSet和testDataSet)
  * 用trainDataSet来构造分类器
  * 用testDataSet来检测分类器的准确率
  */
object HumanSexPredictPro {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NaiveBayesExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawtxt = sc.textFile("Spark/src/main/resources/ml/human-body-features.txt")

    // 将文本文件的内容转化为我们需要的数据结构 LabeledPoint
    val allData = rawtxt.map { line =>
      val colData = line.split(",")
      LabeledPoint(colData(0).toDouble, Vectors.dense(colData(1).split(" ").map(_.toDouble)))
    }

    // 把全部数据按照一定比例分成两份
    val divData = allData.randomSplit(Array(0.7, 0.3), seed = 13L)
    // 一份用来构造分类器
    val trainDataSet = divData(0)
    // 一份用来检测分类器质量
    val testDataSet = divData(1)

    // 训练
    val nbTrained = NaiveBayes.train(trainDataSet)

//    nbTrained.save(sc,"/Users/hushiwei/demo/nb/nb.txt")


    // 根据features预测label
    val nbPredict = nbTrained.predict(testDataSet.map(_.features))

    // 把预测得到的label和实际的label做对比
    val predictionAndLabel = nbPredict.zip(testDataSet.map(_.label))

    // 预测正确的项目占所有项目的比率
    val accuracy = 100.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testDataSet.count()

    println("准确率: " + accuracy)


  }

}
