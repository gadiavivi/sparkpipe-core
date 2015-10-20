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

package software.uncharted.salt.core.analytic.numeric

import org.scalatest._
import software.uncharted.sparkpipe.{Pipe, PipeStage}

class PipeSpec extends FunSpec {
  describe("Pipe (static)") {
    describe("#apply()") {
      it("should allow the creation of a fixed pipe with an input value") {
        val pipe = Pipe("hello")
        assert(pipe.run() == "hello")
      }

      it("should allow the creation of a fixed pipe with an input function") {
        val pipe = Pipe(() => {
          "hello"
        })
        assert(pipe.run() == "hello")
      }

      it("should allow the creation of a fixed pipe by merging two input pipes") {
        val pipe = Pipe(
          Pipe(() => "hello"),
          Pipe(() => "world")
        )
        assert(pipe.run == ("hello", "world"))
      }

      it("should allow the creation of a fixed pipe by merging three input pipes") {
        val pipe = Pipe(
          Pipe(() => "hello"),
          Pipe(() => "world"),
          Pipe(() => "how")
        )
        assert(pipe.run == ("hello", "world", "how"))
      }

      it("should allow the creation of a fixed pipe by merging four input pipes") {
        val pipe = Pipe(
          Pipe(() => "hello"),
          Pipe(() => "world"),
          Pipe(() => "how"),
          Pipe(() => "are")
        )
        assert(pipe.run == ("hello", "world", "how", "are"))
      }

      it("should allow the creation of a fixed pipe by merging five input pipes") {
        val pipe = Pipe(
          Pipe(() => "hello"),
          Pipe(() => "world"),
          Pipe(() => "how"),
          Pipe(() => "are"),
          Pipe(() => "you")
        )
        assert(pipe.run == ("hello", "world", "how", "are", "you"))
      }
    }
  }

  describe("Pipe (instance)") {
    describe("#to()") {
      it("should form a new pipe by connecting the given anonymous function to the tail of the existing pipe") {
        
      }
    }

    describe("#run()") {
      it("should run the given pipe, producing a value") {

      }

      it("should cache results") {

      }
    }

    describe("#reset()") {
      it("should facilitate clearing the cache of previously computed stages") {

      }
    }
  }
}
