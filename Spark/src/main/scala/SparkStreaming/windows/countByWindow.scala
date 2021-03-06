import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 6/23/16.
  */
object countByWindow {
  def main(args: Array[String]) {

    /**
      * this is test  of Streaming operations-----countByWindow
      */
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("the reduceByWindow operation of SparK Streaming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    //set the Checkpoint directory
    ssc.checkpoint("/tmp/sparkstreaming")

    //get the socket Streaming data
    val socketStreaming = ssc.socketTextStream("localhost",9999)

    val data = socketStreaming.countByWindow(Seconds(6),Seconds(2))


    println("countByWindow: count the number of elements")
    data.print()


    ssc.start()
    ssc.awaitTermination()


  }
}
