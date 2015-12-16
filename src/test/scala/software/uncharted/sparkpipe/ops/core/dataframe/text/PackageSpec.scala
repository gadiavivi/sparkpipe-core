/*
 * Copyright 2015 Uncharted Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.uncharted.sparkpipe.ops.core.dataframe.text

import org.scalatest._
import software.uncharted.sparkpipe.Spark
import software.uncharted.sparkpipe.ops.core.rdd.toDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class PackageSpec extends FunSpec {
  describe("ops.core.dataframe.text") {
    val rdd = Spark.sc.parallelize(Seq(
      (1, "lorem ipsum"),
      (2, "lorem ipsum dolor"),
      (3, "lorem ipsum dolor sit"),
      (4, "lorem ipsum dolor sit amet")
    ))
    val df = toDF(Spark.sqlContext)(rdd)

    describe("#stopWordFilter()") {
      it("should remove stop words from the specified column in the input DataFrame") {
        val df2 = stopWordFilter(Set("lorem", "ipsum"), "_2")(df)
        val result = df2.select("_2").collect
        assert(result(0)(0).equals(""))
        assert(result(1)(0).equals("dolor"))
        assert(result(2)(0).equals("dolor sit"))
        assert(result(3)(0).equals("dolor sit amet"))
      }
    }
  }
}
