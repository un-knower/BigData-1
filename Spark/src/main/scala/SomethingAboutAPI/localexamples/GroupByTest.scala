/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package SomethingAboutAPI.localexamples

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
  * Usage: GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]
  */
object GroupByTest {
  def main(args: Array[String]) {
    var numMappers = 10
    var numPartition=10
    var numKVPairs = 100
    var valSize = 100
    var numReducers = 3

    val conf = new SparkConf().setAppName("GroupBy Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //  造一些测试数据
    //    0到10，10次循环，10个分区
    //    每次循环里面造100个键值对
    val pairs1 = sc.parallelize(0 until numMappers, numPartition).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(10), byteArr)
      }
      arr1
    }.cache
    // cache一下
    //    因此一共是10X100=1000个
    println("pairs1.count: "+pairs1.count)

    val result = pairs1.groupByKey(numReducers)
    //    造数据的时候，key的取值是10以内，所以分组肯定是10个，和结果一样
    println("result.count: "+result.count)
    println(result.toDebugString)

    sc.stop()
  }
}
