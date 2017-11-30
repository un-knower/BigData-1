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

package spark.examples.kaggle

import SomethingML.ClickThroughRatePrediction.ClickThroughRatePrediction
import spark.SparkFunSuite
import spark.util.MLlibTestSparkContext

class ClickThroughRatePredictionSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("run") {
    //    Logger.getLogger("org").setLevel(Level.OFF)
    //    Logger.getLogger("akka").setLevel(Level.OFF)

    val trainPath = this.getClass.getResource("/train.part-10000").getPath
    val testPath = this.getClass.getResource("/test.part-10000").getPath
    val resultPath = "./tmp/result/"

    ClickThroughRatePrediction.run(sc, sqlContext, trainPath, testPath, resultPath)
  }

  test("simply function"){

    val trainPath = this.getClass.getResource("/train.part-10000").getPath
    val testPath = this.getClass.getResource("/test.part-10000").getPath
    val resultPath = "./tmp/result/"
    val strings = Array("a","b") //快捷键command+alt+v自动补全
    ClickThroughRatePrediction.runLearning(sc, sqlContext, trainPath, testPath, resultPath)
  }
}
