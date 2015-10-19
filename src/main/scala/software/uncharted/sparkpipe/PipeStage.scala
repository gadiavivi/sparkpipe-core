package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer
/**
 *
 *
 */
class PipeStage[X, Y] (opFunc: X => Y, var parent: Option[PipeStage[_, X]]) {
  private[sparkpipe] val children = new ArrayBuffer[PipeStage[Y,_]]

  // def apply[Y](in: Y): O = {
  //   if (parent.isDefined) {
  //     opFunc(parent.get.apply(in))
  //   } else {
  //     opFunc.asInstanceOf[(Y) => O](in)
  //   }
  // }

  private[sparkpipe] def runStage(in: X): Unit = {
    val out: Y = opFunc(in)
    children.toArray.foreach(f => {
      f.runStage(out)
    })
  }
}
