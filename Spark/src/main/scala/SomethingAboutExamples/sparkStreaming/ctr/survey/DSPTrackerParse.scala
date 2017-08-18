package SomethingAboutExamples.sparkStreaming.ctr.survey

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}

import scala.collection.mutable

/**
  * DSP监播日志的解析类：竞价胜利、展示、点击 3种监播
  * Created by hadoop on 2016/1/9.
  */


object DSPTrackerParse {

  val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

  def parse(map: mutable.Map[String, String], line: String): Unit = {
    try {
      val regex = ".*\\s+\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s+/.*(win|click|imp|dwnl|actv).*\\?(.*)\\s*ALERT:(\\[.+\\])+"
      val p = Pattern.compile(regex)
      val m = p.matcher(line)
      if (m.find()) {
        map += ("eventAction" -> m.group(1))
        parseUri(map, m.group(2))
        parseAlert(map, m.group(3))
        getWinPriceByAdxId(map)
        map += ("bidId" -> (map.get("bidId").get + "_" + map.get("impId").get))
        // cpd结算转化为千次价格
        if ("2".equals(map.get("bidType").get)) {
          map += ("winPrice" -> ((map.get("winPrice").getOrElse("0").toDouble) * 1000).toString)
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  private def getWinPriceByAdxId(map: mutable.Map[String, String]): Unit = {
    val adxId = map.getOrElse("ADXId", "error")
    val winPriceValue = map.getOrElse("winPrice", "0")


    /** ****************************************************
      * # 金立ADX信息 GIONEE_ADX_ID=4
      * *****************************************************/
    val GIONEE_PRICETOKEN = "1a8ed0741c93732d0ae4e87da1721bf4c9bf895b"
    val GIONEE_COMPLETETOKEN = "f50a216d17a0877d83a8781d33edf1d77681872f"

    /** ****************************************************
      * # 酷派ADX信息 MEIZU_ADX_ID=2
      * *****************************************************/
    val COOLPAD_PRICETOKEN = "7eeed2ad8684675095b4a0750162d7ccda6c010e"
    val COOLPAD_COMPLETETOKEN = "b9e88dc70ff932523f33dede5349c4bdb9134cb4"

    /** ****************************************************
      * # 魅族ADX信息 GIONEE_ADX_ID=2
      * *****************************************************/
    //    魅族的不需要解密

    /** ****************************************************
      * # 玩咖自有 WANKA_ADX_ID=2
      * *****************************************************/
//    val WANKA_PRICETOKEN = "ab5ef73883bd6d51b0623959934cf02df60597eb"
//    val WANKA_COMPLETETOKEN = "aa2d67b6a84b5b67d28eecca5b94c4d176c73c2a"

//    测试环境
        val WANKA_PRICETOKEN = "241b302feb255e538b5dad15a7aff0acd24c925b"
        val WANKA_COMPLETETOKEN = "42452f5989de072f14dab736b4b8256de7b1e85f"
    try {
      adxId match {
        case "4" => map += ("winPrice" -> PriceEncryptUtils.decodePrice(GIONEE_PRICETOKEN, GIONEE_COMPLETETOKEN, winPriceValue).toString)
        case "3" => map += ("winPrice" -> PriceEncryptUtils.decodePrice(COOLPAD_PRICETOKEN, COOLPAD_COMPLETETOKEN, winPriceValue).toString)
        case "2" => map += ("winPrice" -> winPriceValue.toString)
        case "1" => map += ("winPrice" -> PriceEncryptUtils.decodePrice(WANKA_PRICETOKEN, WANKA_COMPLETETOKEN, winPriceValue).toString)
        case "error" => map += ("winPrice" -> "0")
        case _ => map += ("winPrice" -> "0")

      }
    } catch {
      case e:Exception => map += ("winPrice" -> "")
    }


  }


  /**
    * 解析每条数据中uri的参数部分 bidrequestid=19282jduewouewere&impid=1
    *
    * @param map
    * @param uri
    */
  def parseUri(map: mutable.Map[String, String], uri: String): Unit = {
    try {
      val uriArr = uri.split("&")
      for (uri: String <- uriArr) {
        val splits: Array[String] = uri.split("=")
        //价格解密

        map += (splits(0).trim() -> splits(1).trim())

      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }


  /**
    * 解析每条数据中的ALERT部分 ALERT:[time:1234567890]
    *
    * @param map
    * @param alert
    */
  def parseAlert(map: mutable.Map[String, String], alert: String): Unit = {
    try {
      val regex = "\\[([^\\]]+)\\]"
      val p: Pattern = Pattern.compile(regex)
      val m: Matcher = p.matcher(alert)
      while (m.find()) {
        val keyValue = m.group(1)
        val splits: Array[String] = keyValue.split(":")
        if ("time".equals(splits(0).trim())) {
          //通过秒级时间生成日期，yyyyMMdd格式，用来设置hive分区表字段
          map += ("day" -> format.format(new Date(splits(1).substring(0, 10).toLong * 1000)))
          map += (splits(0) -> splits(1).substring(0, 10))
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
