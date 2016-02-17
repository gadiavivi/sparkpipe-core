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

package software.uncharted.sparkpipe

import org.scalatest._

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

      it("should allow the creation of a fixed pipe by merging two input pipes, only running the input pipes when the new pipe is run") {
        var pipe1Run = false
        var pipe2Run = false
        val pipe = Pipe(
          Pipe(() => {
            pipe1Run = true
            "hello"
          }),
          Pipe(() => {
            pipe2Run = true
            "world"
          })
        )
        assert(pipe1Run == false)
        assert(pipe2Run == false)
        assert(pipe.run == ("hello", "world"))
        assert(pipe1Run == true)
        assert(pipe2Run == true)
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
        val pipe = Pipe("hello")
        val toPipe = pipe.to((a: String) => a + " world")
        assert(pipe != toPipe)
        assert(toPipe.run() == "hello world")
      }
    }

    describe("#run()") {
      it("should run the given pipe, producing a value") {
        val pipe = Pipe("hello")
        val toPipe = pipe.to(_+" world")
        assert(pipe.run() == "hello")
        assert(toPipe.run() == "hello world")
      }

      it("should cache results") {
        val pipe = Pipe(() => {
          Seq(1,2,3,4)
        }).to(a => {
          a.map(b => b+1)
        })
        val firstRun: Seq[Int] = pipe.run()
        assert(firstRun eq pipe.run())
      }
    }

    describe("#reset()") {
      it("should facilitate clearing the cache of previously computed stages") {
        val pipe = Pipe(() => {
          Seq(1,2,3,4)
        }).to(a => {
          a.map(b => b+1)
        })
        val firstRun: Seq[Int] = pipe.run()
        pipe.reset()
        assert(firstRun ne pipe.run())

        //try something trickier - reset a parent pipe of a pipe
        val pipe2 = pipe.to(a => {
          a.map(b => b+1)
        })
        val secondRun: Seq[Int] = pipe2.run()
        pipe.reset()
        assert(secondRun eq pipe2.run())
      }

      it("should reset parent pipes as well as the current pipe") {
        val pipe1 = Pipe(() => {
          Seq(1,2,3,4)
        })
        val pipe2 = Pipe(() => {
          Seq(2,3,4,5)
        })

        val pipe3 = Pipe(pipe1, pipe2).to(a => {
          a._1 ++ a._2
        })

        val firstPipe1Run: Seq[Int] = pipe1.run()
        val firstPipe2Run: Seq[Int] = pipe2.run()
        val firstPipe3Run: Seq[Int] = pipe3.run()

        pipe3.reset()

        assert(firstPipe1Run ne pipe1.run())
        assert(firstPipe2Run ne pipe2.run())
        assert(firstPipe3Run ne pipe3.run())
      }
    }
  }
}
