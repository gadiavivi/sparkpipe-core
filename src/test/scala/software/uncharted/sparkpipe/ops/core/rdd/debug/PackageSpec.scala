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

package software.uncharted.sparkpipe.ops.core.rdd.debug

import java.io.ByteArrayOutputStream

import org.scalatest._
import software.uncharted.sparkpipe.Spark

class PackageSpec extends FunSpec {
  describe("ops.core.rdd.debug") {
    val rdd = Spark.sc.parallelize(Seq((1, "alpha"), (2, "bravo"), (3, "charlie")))

    describe("#countRDDRows()") {
      it("should output a formatted count message using the supplied output function") {
        var output = ""
        countRDDRows("test", (s: String) => output += s)(rdd)
        assertResult("[test] Number of rows: 3")(output)
      }

      it("should output a formatted count message to std out when no output function is supplied") {
        val bos = new ByteArrayOutputStream()
        Console.withOut(bos) {
          countRDDRows("test")(rdd)
        }
        assertResult("[test] Number of rows: 3\n")(bos.toString)
      }
    }

    describe("#takeRDDRows()") {
      it("should output a list of the first N rows of the rdd") {
        var output = ""
        takeRDDRows(2, "test", (s: String) => output += s)(rdd)
        assertResult("[test] First 2 rows0: (1,alpha)1: (2,bravo)")(output)
      }
    }

    describe("#debugRDDRows()") {
      it("should apply a function to the first N rows of the rdd") {
        var output = Seq[(Int, String)]()
        debugRDDRows(2, (s: Seq[(Int, String)]) => output = s)(rdd)
        assertResult(2)(output.length)
        assertResult(output)(rdd.collect().slice(0, 2).toSeq)
      }
    }
  }
}
