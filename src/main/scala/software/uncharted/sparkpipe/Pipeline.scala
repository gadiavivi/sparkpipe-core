package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer

/**
 *
 *
 */
class Pipeline {
  var root: PipelineStage[_,_] = _

  def start[I, O](opFunc: I => O): PipelineStage[I, O] = {
    //TODO prevent double creation of root with exception
    val result = new PipelineStage[I, O](opFunc, None, this)
    root = result
    result
  }

  def run[I](in : I): Unit = {
    root.asInstanceOf[PipelineStage[I,_]].runStage(in)
  }
}

class PipelineStage[I, O] (opFunc: I => O, parent: Option[PipelineStage[_, I]], pipe: Pipeline) {
  val children = new ArrayBuffer[PipelineStage[O,_]]

  def next[X](opFunc: O => X): PipelineStage[O,X] = {
    val result = new PipelineStage[O,X](opFunc, Some(this), pipe)
    children.append(result)
    result
  }

  def apply[Y](in: Y): O = {
    if (parent.isDefined) {
      opFunc(parent.get.apply(in))
    } else {
      opFunc.asInstanceOf[(Y) => O](in)
    }
  }

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
