package software.uncharted.sparkpipe
import scala.collection.mutable.ArrayBuffer

/**
 *
 *
 */
class PipeStage[X, Y] (
  opFunc: X => Y,
  var parent: Option[PipeStage[_, X]]
) {
  private[sparkpipe] val children = new ArrayBuffer[PipeStage[Y,_]]

  private[sparkpipe] def run[I](in: I): Y = {
    if (parent.isDefined) {
      opFunc(parent.get.run(in))
    } else {
      opFunc.asInstanceOf[(I) => Y](in)
    }
  }
}
