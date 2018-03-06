package SparkStreaming.params

import com.typesafe.config.ConfigFactory

/**
  * Created by Hushiwei on 2017/06/26.
  */
object ConfParms {

  val config = ConfigFactory.load("conf/application.conf")

  /** *****************************   spark    ***********************************/
  val APP_NAME      = config.getString("spark.app_name")
  val MODE          = config.getString("spark.mode")
  val DURATION      = config.getInt("spark.duration")
  val CHECKPOINTDIR = config.getString("spark.checkpointdir")

  /** *****************************   kafka    ***********************************/

  val TOPICS       = config.getString("kafka.topics")
  val BROKERS      = config.getString("kafka.brokers")
  val GROUPID      = config.getString("kafka.groupid")
  val OFFSET_RESET = config.getString("kafka.offset_reset")


  /** *****************************   hbase    ***********************************/

  val ROOTDIR       = config.getString("hbase.rootdir")
  val ZKQUORUM      = config.getString("hbase.zkQuorum")
  val ZKZNODEPARENT = config.getString("hbase.zkZnodeParent")
  val HBASETABLE    = config.getString("hbase.tablename")

  /** *****************************   redis    ***********************************/

  val REDIS_SERVER: String = config.getString("redis.redis_host")
  val REDIS_PORT  : Int    = config.getInt("redis.redis_port")
  val REDIS_PASSWD: String = config.getString("redis.redis_passwd")


}
