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
 * An immutable chain of sequential operations which
 * can be extended, forming a new Pipe. It can also be
 * run, producing an output value which is cached after
 * the first run.
 *
 * @param head the input type for the Pipe (usually Unit)
 * @param tail the tail stage
 * @param parents any parent pipes which this Pipe was constructed from
 *
 */
class Pipe[I,O] private[sparkpipe] (
  private[sparkpipe] val head: I,
  private[sparkpipe] val tail: PipeStage[_,O],
  private[sparkpipe] val parents: Seq[Pipe[_,_]]
) {

  /**
   * @param opFunc a parameter to chain onto the end of this {@link software.uncharted.sparkpipe.Pipe}, forming a new {@link software.uncharted.sparkpipe.Pipe}
   * @tparam A the output type of the new operation
   * @return a new {@link software.uncharted.sparkpipe.Pipe}[I,A], which is the composition of this {@link software.uncharted.sparkpipe.Pipe}[I,O] and the new operation O=>A.
   */
  def to[A](opFunc: O => A): Pipe[I,A] = {
    val next = new PipeStage(opFunc, Some(tail))
    tail.children.append(next)
    new Pipe(head, next, parents)
  }

  /**
   * Run this pipe, producing a value which is cached until reset()
   * @return the output of the tail of this {@link software.uncharted.sparkpipe.Pipe}, cached until reset() is called.
   */
  def run(): O = {
    tail.run(head)
  }

  /**
   * Clears this {@link software.uncharted.sparkpipe.Pipe}'s cache, as well as any parent pipe's cache,
   * forcing the next call to run() to execute from the head of its chain of operations
   */
  def reset(): Unit = {
    tail.reset()
    parents.foreach(_.reset())
  }
}

/**
 * Factory for {@link software.uncharted.sparkpipe.Pipe}s
 * from various inputs, such as raw values or other Pipes
 */
object Pipe {
  /**
   * Create a {@link software.uncharted.sparkpipe.Pipe} from an input value
   * @param first The input value to the {@link software.uncharted.sparkpipe.Pipe}
   * @tparam O The type of the input value for the {@link software.uncharted.sparkpipe.Pipe}
   * @return a {@link software.uncharted.sparkpipe.Pipe}[Unit,O] which represents a closure returning first
   */
  def apply[O](first: O): Pipe[Unit,O] = {
    val wrap: Unit => O = (a: Unit) => {
      first
    }
    new Pipe[Unit,O]((), new PipeStage[Unit,O](wrap, None), Seq())
  }

  /**
   * Create a {@link software.uncharted.sparkpipe.Pipe} from an input value factory
   * @param first A closure which produces the input value to the {@link software.uncharted.sparkpipe.Pipe}. Must take no arguments.
   * @tparam O The type of the input value for the {@link software.uncharted.sparkpipe.Pipe}, produced by first
   * @return a {@link software.uncharted.sparkpipe.Pipe}[Unit,O] which represents the closure first
   */
  def apply[O](first: () => O): Pipe[Unit, O] = {
    val wrap: Unit => O = (a: Unit) => {
      first()
    }
    new Pipe((), new PipeStage[Unit,O](wrap, None), Seq())
  }

  /**
   * Create a {@link software.uncharted.sparkpipe.Pipe} from the outputs of other {@link software.uncharted.sparkpipe.Pipe}s
   * @param first A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type A
   * @param second A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type B
   * @tparam A The output type of first
   * @tparam B The output type of second
   * @return a {@link software.uncharted.sparkpipe.Pipe}[Unit,(A,B)] which can connect the output of first and second to a new set of operations
   */
  def apply[A,B](
    first: Pipe[_,A],
    second: Pipe[_,B]
  ): Pipe[Unit,(A,B)] = {
    val wrap: Unit => (A,B) = (a: Unit) => {
      val firstResult: A = first.run
      val secondResult: B = second.run
      (firstResult, secondResult)
    }
    new Pipe((), new PipeStage[Unit,(A,B)](wrap, None), Seq(first, second))
  }

  /**
   * Create a {@link software.uncharted.sparkpipe.Pipe} from the outputs of other {@link software.uncharted.sparkpipe.Pipe}s
   * @param first A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type A
   * @param second A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type B
   * @param third A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type C
   * @tparam A The output type of first
   * @tparam B The output type of second
   * @tparam C The output type of third
   * @return a {@link software.uncharted.sparkpipe.Pipe}[Unit,(A,B,C)] which can connect the output of first, second and third to a new set of operations
   */
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
    new Pipe((), new PipeStage[Unit,(A,B,C)](wrap, None), Seq(first, second, third))
  }

  /**
   * Create a {@link software.uncharted.sparkpipe.Pipe} from the outputs of other {@link software.uncharted.sparkpipe.Pipe}s
   * @param first A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type A
   * @param second A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type B
   * @param third A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type C
   * @param fourth A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type D
   * @tparam A The output type of first
   * @tparam B The output type of second
   * @tparam C The output type of third
   * @tparam D The output type of fourth
   * @return a {@link software.uncharted.sparkpipe.Pipe}[Unit,(A,B,C,D)] which can connect the output of first, second, third and fourth to a new set of operations
   */
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
    new Pipe((), new PipeStage[Unit,(A,B,C,D)](wrap, None), Seq(first, second, third, fourth))
  }

  /**
   * Create a {@link software.uncharted.sparkpipe.Pipe} from the outputs of other {@link software.uncharted.sparkpipe.Pipe}s
   * @param first A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type A
   * @param second A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type B
   * @param third A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type C
   * @param fourth A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type D
   * @param fifth A {@link software.uncharted.sparkpipe.Pipe} which produes a value of type E
   * @tparam A The output type of first
   * @tparam B The output type of second
   * @tparam C The output type of third
   * @tparam D The output type of fourth
   * @tparam E The output type of fifth
   * @return a {@link software.uncharted.sparkpipe.Pipe}[Unit,(A,B,C,D,E)] which can connect the output of first, second, third, fourth and fifth to a new set of operations
   */
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
    new Pipe((), new PipeStage[Unit,(A,B,C,D,E)](wrap, None), Seq(first, second, third, fourth, fifth))
  }
}
