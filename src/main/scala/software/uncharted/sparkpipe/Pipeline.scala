
import scala.collection.mutable.ArrayBuffer

/**
 *
 *
 */
object Pipeline {
  def op[I, O](opFunc: I => O): PipelineOp[I, O] = {
    new PipelineOp(opFunc)
  }
}

class PipelineOp[I, O](
  opFunc: I => O
) {
  val children = new ArrayBuffer[PipelineOp[O,_]]

  def next[X](opFunc: O => X): PipelineOp[O,X] = {
    val op = new PipelineOp[O,X](opFunc)
    children.append(op)
    op
  }

  def run(in: I): Unit = {
    val out: O = opFunc(in)
    children.toArray.foreach(f => {
      f.run(out)
    })
  }
}
