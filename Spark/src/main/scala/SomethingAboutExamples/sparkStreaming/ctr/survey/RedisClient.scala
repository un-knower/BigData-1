package SomethingAboutExamples.sparkStreaming.ctr.survey

import SomethingAboutExamples.sparkStreaming.params.ConfParms
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * RedisClient
  */
object RedisClient extends Serializable {

  private var MAX_IDLE      : Int     = 200
  private var TIMEOUT       : Int     = 200
  private var TEST_ON_BORROW: Boolean = true

  lazy val config: JedisPoolConfig = {
    val config = new JedisPoolConfig
    config.setMaxIdle(MAX_IDLE)
    config.setTestOnBorrow(TEST_ON_BORROW)
    config
  }

  lazy val pool = new JedisPool(config, ConfParms.REDIS_SERVER,
    ConfParms.REDIS_PORT, TIMEOUT,ConfParms.REDIS_PASSWD)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
