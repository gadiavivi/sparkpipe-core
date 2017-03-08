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

package software.uncharted.sparkpipe.ops.core.dataframe.text.util

import org.scalatest._

class UniqueTermAccumulatorSpec extends FunSpec {
  describe("ops.core.dataframe.text.util.UniqueTermAccumulator") {
    describe("#copy()") {
      it("should copy elements of the accumulator's value by value, not reference") {
        val accumulator = new UniqueTermAccumulator
        accumulator.add(Seq("hello", "world"))
        val copy = accumulator.copy
        copy.add(Seq("goodbye", "cruel", "world"))
        assert(accumulator.value != copy.value)
      }
    }
  }
}
