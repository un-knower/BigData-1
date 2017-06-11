package systemml
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.sysml.api.MLContext

import scala.util.Random
/**
  * Created by HuShiwei on 2016/11/11 0011.
  */
object basic {
  def main(args: Array[String]) {

    val conf= new SparkConf().setAppName("systemML").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val ml = new MLContext(sc)
    val numRows = 10000
    val numCols = 1000
    val data = sc.parallelize(0 to numRows-1).map { _ => Row.fromSeq(Seq.fill(numCols)(Random.nextDouble)) }
    val schema = StructType((0 to numCols-1).map { i => StructField("C" + i, DoubleType, true) } )
    val df = sqlContext.createDataFrame(data, schema)
    df.show()

    val minMaxMean =
      """
minOut = min(Xin)
maxOut = max(Xin)
meanOut = mean(Xin)
      """
//    val mm = new MatrixMetadata(numRows, numCols)
//    val minMaxMeanScript = dml(minMaxMean).in("Xin", df, mm).out("minOut", "maxOut", "meanOut")
//    val (min, max, mean) = ml.execute(minMaxMeanScript).getTuple[Double, Double, Double]("minOut", "maxOut", "meanOut")
  }

}
