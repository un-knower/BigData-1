package dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

//
// Create a DataFrame based on an RDD of case class objects and perform some basic
// DataFrame operations. The DataFrame can instead be created more directly from
// the standard building blocks -- an RDD[Row] and a schema -- see the example
// FromRowsAndSchema.scala to see how to do that.
//
object Basic {
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

    val custs1 = Array(
      Cust(1, "Hu SHIWEI", 120000.00, 100.00, "HUBEI"),
      Cust(2, "TOM", 410500.00, 230.00, "BEIJING"),
      Cust(3, "LENNY", 410500.00, 270.00, "SHANGHAI"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "LINKEN", 500.00, 8.0, "XINJIANG")
    )
    // make it an RDD and convert to a DataFrame

    val customerDF1 = sc.parallelize(custs1, 4).toDF()
    val dff=customerDF.sample(false,0.2)
    customerDF.show()
    dff.show()

    println("-------------------------------------------------")
    customerDF1.cache()
    customerDF.show(truncate = false)
    //    sqlContext.read.text()
    //    DfUtils.df2mysql(customerDF,"hahaha")
    val nameDF = customerDF.select("name")
    nameDF.show()
    nameDF.groupBy()
    nameDF.foreach(x => println(x.getString(0) + "aa"))
    /*    customerDF1.describe().show()

        println("*** toString() just gives you the schema")

        println(customerDF.toString())


        println("*** It's better to use printSchema()")

        customerDF.printSchema()

        println("*** show() gives you neatly formatted data")

        customerDF.show()

        println("*** use select() to choose one column")

        customerDF.select("id").show()

        println("*** use select() for multiple columns")

        customerDF.select("sales", "state").show()

        println("*** use filter() to choose rows")

        customerDF.filter($"state".equalTo("CA")).show()*/


    /*   logger.info("======================")
       customerDF.registerTempTable("table1")
       customerDF1.registerTempTable("table2")
       val df1=sqlContext.sql("select * from table1 ")
       val df2=sqlContext.sql("select * from table2 ")
       sqlContext.sql("select concat('11','22','33')").show()
       //    sqlContext.sql("select * from table1 inner join table2 on table1.id=table2.id ").show()
   //    sqlContext.sql("select table1.name name1,table2.name name2 from table1 inner join table2 on concat(table1.name)=concat(table2.name) ").show()
   //    sqlContext.sql("select SUBSTR(name,1,3) nameT from table1").show()
       println("========================union=======================")
   //    sqlContext.sql("select a.name from table1 a union select b.sales from table2 b").show()
       df1.unionAll(df2).show()*/
  }

}
