package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer

/**
 *
 *
 */
class Pipe[I,O](opFunc: I => O) {
  var head = new PipeStage[I, O](opFunc, None, this)

  def to[X](opFunc: O => X): PipeStage[O,X] = {
    head.to(opFunc)
  }

  def run[I](in : I): Unit = {
    head.asInstanceOf[PipeStage[I,_]].runStage(in)
  }
}

class PipeStage[I, O] (opFunc: I => O, var parent: Option[PipeStage[_, I]], pipe: Pipe[_,_]) {
  val children = new ArrayBuffer[PipeStage[O,_]]

  def to[X](opFunc: O => X): PipeStage[O,X] = {
    //TODO this must be called only once
    val result = new PipeStage[O,X](opFunc, Some(this), pipe)
    children.append(result)
    result
  }

  def to[X](pipes: Pipe[O,_]*): PipeBranchStage[O] = {
    pipes.foreach(p => {
      p.head.parent = Some(this)
      children.append(p.head)
    })
    new PipeBranchStage(this, children)
  }
  class PipeBranchStage[O](parent: PipeStage[_,O], children: ArrayBuffer[PipeStage[O,_]]) {
    def and[X](pipes: Pipe[O,_]*): PipeBranchStage[O] = {
      pipes.foreach(p => {
        p.head.parent = Some(parent)
        children.append(p.head)
      })
      this
    }
  }

  // def apply[Y](in: Y): O = {
  //   if (parent.isDefined) {
  //     opFunc(parent.get.apply(in))
  //   } else {
  //     opFunc.asInstanceOf[(Y) => O](in)
  //   }
  // }

  def run[Y](in: Y): Unit = {
    pipe.run(in)
  }

  private[sparkpipe] def runStage(in: I): Unit = {
    val out: O = opFunc(in)
    children.toArray.foreach(f => {
      f.runStage(out)
    })
  }
}
