/*
 * Copyright 2016 Uncharted Software Inc.
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

package software.uncharted.sparkpipe.ops.core.dataframe.debug

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.scalatest._
import software.uncharted.sparkpipe.{Pipe, Spark}
import software.uncharted.sparkpipe.ops.core.rdd.toDF

import scala.collection.mutable.IndexedSeq

class PackageSpec extends FunSpec {
  describe("ops.core.dataframe.debug") {
    val rdd = Spark.sc.parallelize(Seq((1, "alpha"), (2, "bravo"), (3, "charlie")))
    val df = toDF(Spark.sparkSession)(rdd)

    describe("#countDFRows()") {
      it("should output a formatted count message using the supplied output function") {
        var output = ""
        countDFRows("test", (s: String) => output += s)(df)
        assertResult("[test] Number of rows: 3")(output)
      }
    }

    describe("#takeDFRows()") {
      it("should output a list of the first N rows of the dataframe") {
        var output = ""
        takeDFRows(2, "test", (s: String) => output += s)(df)
        assertResult("[test] First 2 rows0: [1,alpha]1: [2,bravo]")(output)
      }
    }

    describe("#debugDFRows()") {
      it("should apply a function to the first N rows of the dataframe") {
        var output = Seq[Row]()
        debugDFRows(2, (s: Seq[Row]) => output = s)(df)
        assertResult(2)(output.length)
        assertResult(output)(df.collect().slice(0, 2).toSeq)
      }
    }
  }
}
