package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer
/**
 *
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
}

object Pipe {
  def apply[I](first: I) = {
    new Pipe[I,I](first, new PipeStage[I,I]((a: I) => a, None))
  }
  def apply[O](first: () => O) = {
    val wrap: Unit => O = (a: Unit) => {
      first()
    }
    new Pipe((), new PipeStage[Unit,O](wrap, None))
  }

  def from[A,B](first: Pipe[_,A], second: Pipe[_,B]) = {
    val firstResult: A = first.run
    val secondResult: B = second.run
    Pipe((firstResult, secondResult))
  }

  // def from(pipe1: Pipe[], pipe2: Pipe[]): Pipe[] = {
  //
  // }
}
