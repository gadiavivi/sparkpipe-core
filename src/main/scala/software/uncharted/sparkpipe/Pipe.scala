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

package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer

/**
 * An immutable chain of
 *
 */
class Pipe[I,O] private[sparkpipe] (
  val head: I,
  val tail: PipeStage[_,O]
) {

  def to[A](opFunc: O => A): Pipe[I,A] = {
    val next = new PipeStage(opFunc, Some(tail))
    tail.children.append(next)
    new Pipe(head, next)
  }

  def run(): O = {
    tail.run(head)
  }

  def reset(): Unit = {
    tail.reset()
  }
}

/**
 * Facilitates the creation of a new Pipe from various inputs,
 * such as raw values or other Pipes
 */
object Pipe {
  def apply[I](first: I): Pipe[I,I] = {
    new Pipe[I,I](first, new PipeStage[I,I]((a: I) => a, None))
  }
  def apply[O](first: () => O): Pipe[Unit, O] = {
    val wrap: Unit => O = (a: Unit) => {
      first()
    }
    new Pipe((), new PipeStage[Unit,O](wrap, None))
  }

  def apply[O](
    in: Pipe[_,O]
  ): Pipe[Unit,O] = {
    val wrap: Unit => O = (a: Unit) => {
      in.run()
    }
    new Pipe((), new PipeStage[Unit,O](wrap, None))
  }

  def apply[A,B](
    first: Pipe[_,A],
    second: Pipe[_,B]
  ): Pipe[Unit,(A,B)] = {
    val wrap: Unit => (A,B) = (a: Unit) => {
      val firstResult: A = first.run
      val secondResult: B = second.run
      (firstResult, secondResult)
    }
    new Pipe((), new PipeStage[Unit,(A,B)](wrap, None))
  }

  def apply[A,B,C](
    first: Pipe[_,A],
    second: Pipe[_,B],
    third: Pipe[_,C]
  ): Pipe[Unit,(A,B,C)] = {
    val wrap: Unit => (A,B,C) = (a: Unit) => {
      val firstResult: A = first.run
      val secondResult: B = second.run
      val thirdResult: C = third.run
      (firstResult, secondResult, thirdResult)
    }
    new Pipe((), new PipeStage[Unit,(A,B,C)](wrap, None))
  }

  def apply[A,B,C,D](
    first: Pipe[_,A],
    second: Pipe[_,B],
    third: Pipe[_,C],
    fourth: Pipe[_,D]
  ): Pipe[Unit,(A,B,C,D)] = {
    val wrap: Unit => (A,B,C,D) = (a: Unit) => {
      val firstResult: A = first.run
      val secondResult: B = second.run
      val thirdResult: C = third.run
      val fourthResult: D = fourth.run
      (firstResult, secondResult, thirdResult, fourthResult)
    }
    new Pipe((), new PipeStage[Unit,(A,B,C,D)](wrap, None))
  }

  def apply[A,B,C,D,E](
    first: Pipe[_,A],
    second: Pipe[_,B],
    third: Pipe[_,C],
    fourth: Pipe[_,D],
    fifth: Pipe[_,E]
  ): Pipe[Unit,(A,B,C,D,E)] = {
    val wrap: Unit => (A,B,C,D,E) = (a: Unit) => {
      val firstResult: A = first.run
      val secondResult: B = second.run
      val thirdResult: C = third.run
      val fourthResult: D = fourth.run
      val fifthResult: E = fifth.run
      (firstResult, secondResult, thirdResult, fourthResult, fifthResult)
    }
    new Pipe((), new PipeStage[Unit,(A,B,C,D,E)](wrap, None))
  }
}
