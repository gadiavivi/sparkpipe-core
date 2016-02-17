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
import org.apache.spark.{AccumulableParam, Accumulable}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer

/**
 * An accumulable for MultivariateOnlineSummarizers (one per column in a DataFrame)
 */
private[numeric] class MultivariateOnlineSummarizerAccumulable(val initialValue: Seq[MultivariateOnlineSummarizer])
extends Accumulable[Seq[MultivariateOnlineSummarizer], Row](initialValue, new MultivariateOnlineSummarizerAccumulableParam)

/**
 * An AccumulableParam for MultivariateOnlineSummarizers (one per column in a DataFrame)
 */
private[numeric] class MultivariateOnlineSummarizerAccumulableParam extends AccumulableParam[Seq[MultivariateOnlineSummarizer], Row]() {

  override def addAccumulator(r: Seq[MultivariateOnlineSummarizer], t: Row): Seq[MultivariateOnlineSummarizer] = {
    for (i <- 0 to t.length-1) {
      if (!t.isNullAt(i)) {
        r(i).add(Vectors.dense(Array[Double](t.getDouble(i))))
      } else {
        // don't add a sample to the summarizer for this column
      }
    }
    r
  }

  override def addInPlace(r1: Seq[MultivariateOnlineSummarizer], r2: Seq[MultivariateOnlineSummarizer]): Seq[MultivariateOnlineSummarizer] = {
    for (i <- 0 to r1.length-1) {
      r1(i).merge(r2(i))
    }
    r1
  }

  override def zero(initialValue: Seq[MultivariateOnlineSummarizer]): Seq[MultivariateOnlineSummarizer] = {
    initialValue
  }
}
