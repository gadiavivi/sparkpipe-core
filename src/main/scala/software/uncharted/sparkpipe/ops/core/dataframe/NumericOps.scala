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

package software.uncharted.sparkpipe.ops.core.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row, Column}
import org.apache.spark.sql.types.{FloatType, DoubleType, IntegerType, LongType, TimestampType, DateType}
import software.uncharted.sparkpipe.ops.core.DataFrameOps

object NumericOps {
  private val supportedColumnTypes = List("FloatType", "DoubleType", "IntegerType", "LongType", "TimestampType", "DateType")

  /**
   * Convert all compatible columns within a DataFrame into Doubles
   *
   * @param input Input DataFrame to convert
   * @throws IllegalArgumentException if the input DataFrame does not contain any compatible columns
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
          s"cast(cast(`${name}` as timestamp) as double)"
        } else {
          s"cast(`${name}` as double)"
        }
      })
      input.selectExpr(query:_*)
    }
  }
}
