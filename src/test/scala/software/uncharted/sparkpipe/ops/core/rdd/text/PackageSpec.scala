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

package software.uncharted.sparkpipe.ops.core.rdd.text

import org.scalatest.FunSpec
import software.uncharted.sparkpipe.Spark

class PackageSpec extends FunSpec {

  describe("ops.core.rdd.text") {
    describe("#regexFilter()") {
      it("Should pass through only strings that match the given regular expression") {
        val data = Spark.sc.parallelize(Seq("abc def ghi", "abc d e f ghi", "the def quick", "def ghi", "abc def"))
        assertResult(List("abc def ghi", "the def quick", "def ghi"))(regexFilter(".*def.+")(data).collect.toList)
        assertResult(List("abc d e f ghi", "def ghi"))(regexFilter(".+def.*", true)(data).collect.toList)
      }
    }
  }
}
