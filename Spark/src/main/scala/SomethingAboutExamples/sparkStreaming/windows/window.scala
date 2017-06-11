import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object  windowOnStreaming {
  def main(args: Array[String]) {
    /**
      * this is test  of Streaming operations-----window
      */
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("the Window operation of SparK Streaming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))


    //set the Checkpoint directory
    ssc.checkpoint("/Res")

    //get the socket Streaming data
    val socketStreaming = ssc.socketTextStream("master",9999)

    val data = socketStreaming.map(x =>(x,1))
    //def window(windowDuration: Duration): DStream[T]
    val getedData1 = data.window(Seconds(6))
    println("windowDuration only : ")
    getedData1.print()
    //ssame as
    // def window(windowDuration: Duration, slideDuration: Duration): DStream[T]
    //val getedData2 = data.window(Seconds(9),Seconds(3))
    //println("Duration and SlideDuration : ")
    //getedData2.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
