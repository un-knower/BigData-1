package experiments

import org.apache.spark.sql.SQLContext
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object CoPartitioning {

  case class Customer(id: Integer, name: String, state: String)

  case class Order(id: Integer, custid: Integer, sku: String, quantity: Integer)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-Basic").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val custs = Seq(
      Customer(1, "Widget Co", "AZ"),
      Customer(2, "Acme Widgets", "CA"),
      Customer(3, "Widgetry", "CA"),
      Customer(4, "Widgets R Us", "CA"),
      Customer(5, "Ye Olde Widgete", "MA")
    )

    val orders = Seq(
      Order(1001, 1, "A001", 100),
      Order(1002, 1, "B002", 500),
      Order(1003, 3, "A001", 100),
      Order(1004, 3, "B001", 100),
      Order(1005, 4, "A002", 10),
      Order(1006, 5, "A003", 100)
    )

    val cc = sc.parallelize(custs, 4)
    cc.toDF().registerTempTable("custs1")

    sc.parallelize(orders, 2).toDF().registerTempTable("orders1")

    val res1 = sqlContext.sql("SELECT * FROM custs1 INNER JOIN orders1 ON custs1.id = orders1.custid")

    res1.explain()

    res1.show()

    val partitioner = new HashPartitioner(10)

    val custs2 = sqlContext.sql("SELECT * from custs1").map(r => (r.getInt(0), r)).partitionBy(partitioner)

    val orders2 = sqlContext.sql("SELECT * from orders1").map(r => (r.getInt(1), r)).partitionBy(partitioner)

    custs2.map({case (k,r) => Customer(r.getInt(0), r.getString(1), r.getString(2))}).toDF().registerTempTable("custs2")

    println(custs2.partitions.size)

    orders2.map({case (k,r) => Order(r.getInt(0), r.getInt(1), r.getString(2), r.getInt(3))}).toDF().registerTempTable("orders2")

    println(orders2.partitions.size)

    println(custs2.join(orders2).partitions.size)

    val res2 = sqlContext.sql("SELECT * FROM custs2 INNER JOIN orders2 ON custs2.id = orders2.custid")

    res2.explain()

    res2.show()

  }
}
