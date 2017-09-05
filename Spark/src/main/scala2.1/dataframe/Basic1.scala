package dataframe

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

//
// Create a DataFrame based on an RDD of case class objects and perform some basic
// DataFrame operations. The DataFrame can instead be created more directly from
// the standard building blocks -- an RDD[Row] and a schema -- see the example
// FromRowsAndSchema.scala to see how to do that.
//
object Basic1 {
  private val logger: Logger = LoggerFactory.getLogger(Basic.getClass.getName)

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-Basic").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // create a sequence of case class objects
    // (we defined the case class above)
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val customerDF = sc.parallelize(custs, 4).toDF()


    val map=Map("1"->"hello","2"->"world")

    val ds=customerDF.map(cols=>{
      Row(cols.get(0),map.getOrElse(cols.get(0).toString,"heheda"))
    })
    ds.foreach(println)

  }

}
