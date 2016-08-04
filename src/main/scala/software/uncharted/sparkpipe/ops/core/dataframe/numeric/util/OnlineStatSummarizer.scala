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

import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer

private[numeric] class OnlineStatSummarizer extends MultivariateOnlineSummarizer {
  def copy(): OnlineStatSummarizer = {
    val result = new OnlineStatSummarizer
    copyField("n", result)
    copyField("currMean", result)
    copyField("currM2n", result)
    copyField("currM2", result)
    copyField("currL1", result)
    copyField("totalCnt", result)
    copyField("totalWeightSum", result)
    copyField("weightSquareSum", result)
    copyField("weightSum", result)
    copyField("nnz", result)
    copyField("currMax", result)
    copyField("currMin", result)
    result
  }

  private def copyField(name: String, dest: OnlineStatSummarizer): Unit = {
    val field = classOf[MultivariateOnlineSummarizer].getDeclaredField(name)
    field.setAccessible(true)
    if (field.getType().equals(classOf[Array[_]])) {
      field.set(dest, field.get(this).asInstanceOf[Array[_]].clone)
    } else {
      field.set(dest, field.get(this))
    }
    field.setAccessible(false)
  }
}
