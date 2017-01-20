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

package software.uncharted.sparkpipe.ops.core.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame, Row, Column}
import org.apache.spark.sql.types.{FloatType, DoubleType, IntegerType, LongType, TimestampType, DateType}
import software.uncharted.sparkpipe.ops.core.dataframe.numeric.util.{MultivariateOnlineSummarizerAccumulator, SummaryStats}
import java.lang.IllegalArgumentException

/**
 * Numeric pipeline operations which operate on DataFrames which have columns of the following types:
 *
 * - FloatType
 * - DoubleType
 * - IntegerType
 * - LongType
 * - DateType
 * - TimestampType
 */
package object numeric {
  private val supportedColumnTypes = List("FloatType", "DoubleType", "IntegerType", "LongType", "DateType", "TimestampType")

  /**
   * Convert all compatible columns within a DataFrame into Doubles
   *
   * @param input Input DataFrame to convert
   * @throws java.lang.IllegalArgumentException if the input DataFrame does not contain any compatible columns
   * @return Transformed DataFrame, where all suitable columns have been converted to Doubles,
   *         and incompatible columns have been dropped.
   */
  def enumerate(input: DataFrame): DataFrame = {
    val typeKey = "type"
    val fields = input.schema.fields
    val schema = fields.map(i => Map("name"->i.name, typeKey->i.dataType.toString))
    val columns = schema.filter(col => supportedColumnTypes.contains(col(typeKey)))

    if (columns.isEmpty) {
      throw new IllegalArgumentException("Input DataFrame does not contain any columns which can be converted to Doubles")
    } else {
      //build a select array to filter our data down to only the compatible columns
      val query = columns.map(t => {
        val name = t("name")
        if (t(typeKey) == "DateType") {
          s"cast(cast(`${name}` as timestamp) as double) as ${name}"
        } else {
          s"cast(`${name}` as double) as ${name}"
        }
      })
      input.selectExpr(query:_*)
    }
  }

  /**
   * Computes summary statistics using online algorithms for each compatible
   * column in an input DataFrame. Ignores null fields within a row without
   * causing the summarizer for that column to return NaN. Statistics returned include:
   * - min
   * - max
   * - mean
   * - variance
   * - normL1
   * - normL2
   * - numNonzeros
   *
   * @param input Input DataFrame to analyze
   * @return a Seq[(String, OnlineStatSummarizer)], with one OnlineStatSummarizer per column (paired with the column name)
   */
  def summaryStats(sc: SparkContext)(input: DataFrame): Seq[SummaryStats] = {
    // extract compatible columns
    val df = enumerate(input)
    val cols = df.schema

    // and an accumulator for the summarizers, so that the process can be parallelized
    val accumulator = new MultivariateOnlineSummarizerAccumulator(cols)
    sc.register(accumulator)
    // accumulate each row
    df.foreach(row => {
      accumulator.add(row)
    })

    //produce summary stats structure and return
    for ((col, i) <- cols.view.zipWithIndex) yield (
      new SummaryStats(
        col.name,
        accumulator.value(i).count,
        accumulator.value(i).min(0),
        accumulator.value(i).mean(0),
        accumulator.value(i).max(0),
        accumulator.value(i).normL1(0),
        accumulator.value(i).normL2(0),
        accumulator.value(i).variance(0),
        accumulator.value(i).numNonzeros(0)
      )
    )
  }
}
