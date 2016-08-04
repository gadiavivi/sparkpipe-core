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
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer

private object MultivariateOnlineSummarizerAccumulator {
  def init(cols: Seq[_]): Seq[MultivariateOnlineSummarizer] = {
    cols.map(col => {
      new MultivariateOnlineSummarizer
    }).toSeq
  }
}

/**
 * An accumulator for MultivariateOnlineSummarizers (one per column in a DataFrame)
 */
private[numeric] class MultivariateOnlineSummarizerAccumulator(
  private var result: Seq[MultivariateOnlineSummarizer],
  private var touched: Boolean = false
) extends AccumulatorV2[Row, Seq[MultivariateOnlineSummarizer]] {

  def this(cols: StructType) {
    this(MultivariateOnlineSummarizerAccumulator.init(cols))
  }

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

  override def copy(): AccumulatorV2[Row, Seq[MultivariateOnlineSummarizer]] = {
    new MultivariateOnlineSummarizerAccumulator(result.map(s => {
      // clone by making a new, empty summarizer and merging our data into it
      val newSummarizer = new MultivariateOnlineSummarizer()
      newSummarizer.merge(s)
      newSummarizer
    }), false)
  }

  override def isZero(): Boolean = {
    !touched
  }

  override def merge(other: AccumulatorV2[Row, Seq[MultivariateOnlineSummarizer]]): Unit = {
    for (i <- 0 to other.value.length-1) {
      result(i).merge(other.value(i))
    }
  }

  override def reset(): Unit = {
    result = MultivariateOnlineSummarizerAccumulator.init(result)
    touched = false
  }

  override def value: Seq[MultivariateOnlineSummarizer] = {
    result
  }
}
