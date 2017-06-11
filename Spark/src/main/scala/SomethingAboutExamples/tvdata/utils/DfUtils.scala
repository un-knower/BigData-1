package SomethingAboutExamples.tvdata.utils


import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.control.NonFatal

/**
  * Created by HuShiwei on 2016/9/8 0008.
  */
object DfUtils {

  def df2mysql(df: DataFrame, tableName: String) = {
    //    import sqlContext.implicits._
    //val tvDataFrame = data.toDF()
    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "5Rb!!@bqC%")
    df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.15.15:3306/sparkSQL", tableName, prop)
  }

  def df2mysql2(df: DataFrame, tableName: String) = {
//    以下是用来建表的,表存在,所以我先注释了
/*    val rddschema: StructType = df.schema
    val structFields: Array[StructField] = rddschema.fields
    val createTable: StringBuilder = new StringBuilder
    createTable.append("create table ").append("if not exists ").append(tableName).append(" (")
    var i: Int = 0
    while (i < structFields.length) {
      {
        val colName: String = structFields(i).name
        val datatype: String = structFields(i).dataType.toString
        var colType: String = null
        datatype match {
          case "StringType" => colType = "varchar(255)"
          case "IntegerType" => colType = "INTEGER"
          case "DoubleType" => colType = "DOUBLE PRECISION"
          case "FloatType" => colType = "REAL"
          case "ShortType" => colType = "INTEGER"
          case "ByteType" => colType="BYTE"
          case "BooleanType" => colType="BIT(1)"
          case "BinaryType" => colType="BLOB"
          case "TimestampType" => colType="TIMESTAMP"
          case "DateType" => colType="DATE"
          case "BinaryType" => colType="BLOB"
          case _ => colType = "varchar(255)"
        }
        createTable.append(colName).append(" ").append(colType)
        if (i < structFields.length - 1) {
          createTable.append(",")
        }
        i += 1
      }
    }
    createTable.append(") ")
    println("====> " + createTable.toString)*/

    val rddSchema = df.schema
    val iterator = df.toJavaRDD.collect().iterator()

    val nullTypes: Array[Int] = null
    var conn: Connection = null
    //      初始化
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://192.168.15.15:3306/sparkSQL", "root", "5Rb!!@bqC%")
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        println("Exception while detecting transaction support", e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false)
      }
      //        调用插入方法，匹配dataframe中的类型，插入指定的类型
      val stmt = insertStatement(conn, tableName, rddSchema)
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
              }
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % 1000 == 0) {
            stmt.executeBatch()
            println("===> 开始插入1000条")
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      }
      finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => println("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }


  def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val sql = new StringBuilder(s"INSERT INTO $table VALUES (")
    var fieldsLeft = rddSchema.fields.length
    while (fieldsLeft > 0) {
      sql.append("?")
      if (fieldsLeft > 1) sql.append(", ") else sql.append(")")
      fieldsLeft = fieldsLeft - 1
    }
    conn.prepareStatement(sql.toString())
  }
}
