package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer
/**
 *
 *
 */
class Pipe[I,O] private[sparkpipe] (
  val head: PipeStage[I, _],
  val tail: PipeStage[_, O]
) {
  private[sparkpipe] def this(first: PipeStage[I,O]) = {
    this(first, first)
  }

  def this(first: I => O) = {
    this(new PipeStage[I,O](first, None))
  }

  def to[A](opFunc: O => A): Pipe[I,A] = {
    val next = new PipeStage(opFunc, Some(tail))
    tail.children.append(next)
    new Pipe(head, next)
  }

  def run(in : I): Unit = {
    head.runStage(in)
  }
}

// object Pipe {
//   def from(pipe1: Pipe[], pipe2: Pipe[]): Pipe[] = {
//
//   }
// }
