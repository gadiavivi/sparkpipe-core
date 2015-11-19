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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{udf, callUDF}
import org.apache.spark.sql.types.DataType

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

  /**
   * cache() the specified DataFrame
   * @param frame the DataFrame to cache()
   * @return the input DataFrame, after calling cache()
   */
  def cache(frame: DataFrame): DataFrame = {
    frame.cache()
    frame
  }

  /**
   * Remove a column from a DataFrame
   * @param colName the column to remove
   * @param input the input DataFrame
   * @return the resultant DataFrame, without the specified column
   */
  def dropColumn(colName: String)(input: DataFrame): DataFrame = {
    input.drop(colName)
  }

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnType the type of the column to add
   * @param columnFcn A function mapping the values of the base data specified by inputColumns onto an output value,
   *                  which had darn well better be of the right type.
   * @param inputColumns The input columns needed to calculate the output column; their extracted values become the
   *                     inputs to columnFcn
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  def addColumn (columnName: String, columnType: DataType,
                 columnFcn: Array[Any] => Any, inputColumns: String*)(input: DataFrame): DataFrame = {
    val columns = inputColumns.map(new Column(_))
    val newColumn = inputColumns.length match {
      case  0 =>
        callUDF(() => columnFcn(Array[Any]()),
                columnType)
      case  1 =>
        callUDF((a: Any) => columnFcn(Array[Any](a)),
                columnType,
                columns(0))
      case  2 =>
        callUDF((a: Any, b: Any) => columnFcn(Array[Any](a, b)),
                columnType,
                columns(0), columns(1))
      case  3 =>
        callUDF((a: Any, b: Any, c: Any) => columnFcn(Array[Any](a, b, c)),
                columnType,
                columns(0), columns(1), columns(2))
      case  4 =>
        callUDF((a: Any, b: Any, c: Any, d: Any) => columnFcn(Array[Any](a, b, c, d)),
                columnType,
                columns(0), columns(1), columns(2), columns(3))
      case  5 =>
        callUDF((a: Any, b: Any, c: Any, d: Any, e: Any) => columnFcn(Array[Any](a, b, c, d, e)),
                columnType,
                columns(0), columns(1), columns(2), columns(3), columns(4))
    }
    input.withColumn(columnName, newColumn)
  }

  /**
   * Rename a column from a DataFrame
   * @param nameMap a Map[String, String] from columns in the DataFrame to new names
   * @param input the input DataFrame
   * @return a new DataFrame with the renamed column
   */
  def renameColumn(nameMap: Map[String, String])(input: DataFrame): DataFrame = {
    var cur = input;
    nameMap.foreach(m => {
      cur = cur.withColumnRenamed(m._1, m._2)
    })
    cur
  }

  // TODO cast column

  /**
   * Bring in temporal ops so they can be referred to with dot notation
   */
  val temporal = dataframe.TemporalOps
}
