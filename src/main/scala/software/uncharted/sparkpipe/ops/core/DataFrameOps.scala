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

package software.uncharted.sparkpipe.ops.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import scala.reflect.runtime.universe._

/**
 * Core pipeline operations for working with DataFrames
 */
object DataFrameOps {
  /**
   * Convert a DataFrame to an RDD[Row]
   * @param frame the DataFrame
   * @return the underlying RDD[Row] from frame
   */
  def toRDD(frame: DataFrame): RDD[Row] = {
    frame.rdd
  }
}
