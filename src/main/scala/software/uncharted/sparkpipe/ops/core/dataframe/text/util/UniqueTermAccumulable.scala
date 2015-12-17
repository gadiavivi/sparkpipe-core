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

package software.uncharted.sparkpipe.ops.core.dataframe.text.util

import org.apache.spark.sql.Row
import org.apache.spark.{AccumulableParam, Accumulable}
import scala.collection.mutable.HashMap

/**
 * An accumulable for listing unique terms along with counts from an Array[String] column
 */
private[text] class UniqueTermAccumulable(val initialValue: HashMap[String, Int])
extends Accumulable[HashMap[String, Int], Seq[String]](initialValue, new UniqueTermAccumulableParam)

/**
 * An AccumulableParam for UniqueTerms (one per column in a DataFrame)
 */
private[text] class UniqueTermAccumulableParam extends AccumulableParam[HashMap[String, Int], Seq[String]]() {

  override def addAccumulator(r: HashMap[String, Int], t: Seq[String]): HashMap[String, Int] = {
    t.foreach(w => {
      r.put(w, r.getOrElse(w, 0) + 1)
    })
    r
  }

  override def addInPlace(r1: HashMap[String, Int], r2: HashMap[String, Int]): HashMap[String, Int] = {
    r2.foreach(t => {
      r1.put(t._1, r1.getOrElse(t._1, 0) + t._2)
    })
    r1
  }

  override def zero(initialValue: HashMap[String, Int]): HashMap[String, Int] = {
    new HashMap[String, Int]()
  }
}
