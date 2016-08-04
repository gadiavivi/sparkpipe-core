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

package software.uncharted.sparkpipe.ops.core.dataframe.numeric.util

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.sql.types.StructType

private object OnlineStatSummarizerAccumulator {
  def init(cols: Seq[_]): Seq[OnlineStatSummarizer] = {
    cols.map(col => {
      new OnlineStatSummarizer
    }).toSeq
  }
}

/**
 * An accumulator for OnlineStatSummarizers (one per column in a DataFrame)
 */
private[numeric] class OnlineStatSummarizerAccumulator(var result: Seq[OnlineStatSummarizer])
  extends AccumulatorV2[Row, Seq[OnlineStatSummarizer]] {

  def this(cols: StructType) {
    this(OnlineStatSummarizerAccumulator.init(cols))
  }

  private var touched = false

  override def add(r: Row): Unit = {
    for (i <- 0 to r.length-1) {
      if (!r.isNullAt(i)) {
        result(i).add(Vectors.dense(Array[Double](r.getDouble(i))))
        touched = true
      } else {
        // don't add a sample to the summarizer for this column
      }
    }
  }

  override def copy(): AccumulatorV2[Row, Seq[OnlineStatSummarizer]] = {
    new OnlineStatSummarizerAccumulator(result.map(s => s.copy))
  }

  override def isZero(): Boolean = {
    touched
  }

  override def merge(other: AccumulatorV2[Row, Seq[OnlineStatSummarizer]]): Unit = {
    for (i <- 0 to other.value.length-1) {
      result(i).merge(other.value(i))
    }
  }

  override def reset(): Unit = {
    result = OnlineStatSummarizerAccumulator.init(result)
  }

  override def value: Seq[OnlineStatSummarizer] = {
    result
  }
}
