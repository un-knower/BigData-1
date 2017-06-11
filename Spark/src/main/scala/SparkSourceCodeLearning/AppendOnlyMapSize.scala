package SparkSourceCodeLearning

import com.google.common.hash.Hashing

/**
  * Created by HuShiwei on 2016/9/21 0021.
  */
object AppendOnlyMapSize {
  def main(args: Array[String]) {
    val MAXIMUM_CAPACITY = (1 << 29)
    println("AppendOnlyMapSize: "+MAXIMUM_CAPACITY)

    val key="hello"
    var pos=rehash(key.hashCode.toLong)
    println(pos)
    println(rehash(pos))
  }
  private def rehash(h: Long): Int = Hashing.murmur3_32().hashLong(h).asInt()

}
