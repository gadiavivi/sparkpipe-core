/*
 * Copyright 2016 Uncharted Software Inc.
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
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.HashMap

/**
 * An accumulator for listing unique terms along with counts from an Array[String] column
 */
private[text] class UniqueTermAccumulator(
  private var result: HashMap[String, Int],
  private var touched: Boolean = false
) extends AccumulatorV2[Seq[String], HashMap[String, Int]] {

  def this() {
    this(new HashMap[String, Int]())
  }

  override def add(in: Seq[String]): Unit = {
    in.foreach(w => {
      result.put(w, result.getOrElse(w, 0) + 1)
    })
  }

  override def copy(): AccumulatorV2[Seq[String], HashMap[String, Int]] = {
    val clone = new HashMap[String, Int]()
    result.foreach(kv => clone.put(kv._1, kv._2))
    new UniqueTermAccumulator(clone, false)
  }

  override def isZero(): Boolean = {
    !touched
  }

  override def merge(other: AccumulatorV2[Seq[String], HashMap[String, Int]]): Unit = {
    other.value.foreach(t => {
      result.put(t._1, result.getOrElse(t._1, 0) + t._2)
    })
  }

  override def reset(): Unit = {
    result.clear
    touched = false
  }

  override def value: HashMap[String, Int] = {
    result
  }
}
