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
 *
 *
 */
class PipeStage[X, Y] private[sparkpipe] (
  opFunc: X => Y,
  var parent: Option[PipeStage[_, X]]
) {
  private var cache: Option[Y] = None

  private[sparkpipe] val children = new ArrayBuffer[PipeStage[Y,_]]

  private[sparkpipe] def reset(): Unit = {
    cache = None
    if (parent.isDefined) {
      parent.get.reset()
    }
  }

  private[sparkpipe] def run[I](in: I): Y = {
    if (cache.isDefined) {
      cache.get
    } else if (parent.isDefined) {
      val result = opFunc(parent.get.run(in))
      cache = Some(result)
      result
    } else {
      val result = opFunc.asInstanceOf[(I) => Y](in)
      cache = Some(result)
      result
    }
  }
}
