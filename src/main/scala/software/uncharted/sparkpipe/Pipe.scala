package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer
/**
 *
 *
 */
class Pipe[I,O](val head: PipeStage[I, _], val tail: PipeStage[_, O]) {

  def this(first: PipeStage[I,O]) = {
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

  def split[A](pipes: Pipe[O,A]*): PipeBranchStage[I] = {
    pipes.foreach(p => {
      p.head.parent = Some(tail)
      tail.children.append(p.head)
    })
    new PipeBranchStage(this)
  }
  class PipeBranchStage[X](pipe: Pipe[X,_]) {
    def run(in: X): Unit = {
      pipe.run(in)
    }
  }

  def run(in : I): Unit = {
    head.runStage(in)
  }
}
