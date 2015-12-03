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

import software.uncharted.sparkpipe.Pipe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * Common operations for manipulating dataframes
 */
package object dataframe {
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
  }

  /**
   * Remove columns from a DataFrame
   * @param colNames the named columns to remove
   * @param input the input DataFrame
   * @return the resultant DataFrame, without the specified column
   */
  def dropColumns(colNames: String*)(input: DataFrame): DataFrame = {
    var cur = input
    colNames.foreach(c => {
      cur = cur.drop(c)
    })
    cur
  }

  /**
   * Rename columns in a DataFrame
   * @param nameMap a Map[String, String] from columns in the DataFrame to new names
   * @param input the input DataFrame
   * @return a new DataFrame with the renamed column
   */
  def renameColumns(nameMap: Map[String, String])(input: DataFrame): DataFrame = {
    var cur = input;
    nameMap.foreach(m => {
      cur = cur.withColumnRenamed(m._1, m._2)
    })
    cur
  }

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnFcn A function which generates the new column's values based on input columns
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  def addColumn[O]
    (columnName: String, columnFcn: () => O)
    (input: DataFrame)
    (implicit tag: TypeTag[O]):DataFrame = {
      val newColumn = udf {columnFcn}(tag)()
      input.withColumn(columnName, newColumn)
  }

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnFcn A function which generates the new column's values based on input columns
   * @param in the input column
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  def addColumn[I, O]
    (columnName: String, columnFcn: (I) => O, in: String)
    (input: DataFrame)
    (implicit tagO: TypeTag[O], tagI: TypeTag[I]):DataFrame = {
      val newColumn = udf {columnFcn}(tagO, tagI)(new Column(in))
      input.withColumn(columnName, newColumn)
  }

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnFcn A function which generates the new column's values based on input columns
   * @param in1 the first input column
   * @param in2 the second input column
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  def addColumn[I1, I2, O]
    (columnName: String, columnFcn: (I1, I2) => O, in1: String, in2: String)
    (input: DataFrame)
    (implicit tagO: TypeTag[O], tagI1: TypeTag[I1], tagI2: TypeTag[I2]):DataFrame = {
      val newColumn = udf {columnFcn}(tagO, tagI1, tagI2)(
        new Column(in1),
        new Column(in2)
      )
      input.withColumn(columnName, newColumn)
  }

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnFcn A function which generates the new column's values based on input columns
   * @param in1 the first input column
   * @param in2 the second input column
   * @param in3 the third input column
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  // scalastyle:off parameter.number
  def addColumn[I1, I2, I3, O]
    (columnName: String, columnFcn: (I1, I2, I3) => O, in1: String, in2: String, in3: String)
    (input: DataFrame)
    (implicit tagO: TypeTag[O], tagI1: TypeTag[I1], tagI2: TypeTag[I2], tagI3: TypeTag[I3]):DataFrame = {
      val newColumn = udf {columnFcn}(tagO, tagI1, tagI2, tagI3)(
        new Column(in1),
        new Column(in2),
        new Column(in3)
      )
      input.withColumn(columnName, newColumn)
  }
  // scalastyle:on parameter.number

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnFcn A function which generates the new column's values based on input columns
   * @param in1 the first input column
   * @param in2 the second input column
   * @param in3 the third input column
   * @param in4 the fourth input column
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  // scalastyle:off parameter.number
  def addColumn[I1, I2, I3, I4, O]
    (columnName: String, columnFcn: (I1, I2, I3, I4) => O, in1: String, in2: String, in3: String, in4: String)
    (input: DataFrame)
    (implicit tagO: TypeTag[O], tagI1: TypeTag[I1], tagI2: TypeTag[I2], tagI3: TypeTag[I3], tagI4: TypeTag[I4]):DataFrame = {
      val newColumn = udf {columnFcn}(tagO, tagI1, tagI2, tagI3, tagI4)(
        new Column(in1),
        new Column(in2),
        new Column(in3),
        new Column(in4)
      )
      input.withColumn(columnName, newColumn)
  }
  // scalastyle:on parameter.number

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnFcn A function which generates the new column's values based on input columns
   * @param in1 the first input column
   * @param in2 the second input column
   * @param in3 the third input column
   * @param in4 the fourth input column
   * @param in5 the fifth input column
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  // scalastyle:off parameter.number
  def addColumn[I1, I2, I3, I4, I5, O]
    (columnName: String, columnFcn: (I1, I2, I3, I4, I5) => O, in1: String, in2: String, in3: String, in4: String, in5: String)
    (input: DataFrame)
    (implicit tagO: TypeTag[O], tagI1: TypeTag[I1], tagI2: TypeTag[I2], tagI3: TypeTag[I3], tagI4: TypeTag[I4], tagI5: TypeTag[I5]):DataFrame = {
      val newColumn = udf {columnFcn}(tagO, tagI1, tagI2, tagI3, tagI4, tagI5)(
        new Column(in1),
        new Column(in2),
        new Column(in3),
        new Column(in4),
        new Column(in5)
      )
      input.withColumn(columnName, newColumn)
  }
  // scalastyle:on parameter.number

  /**
   * Takes a DataFrame and replaces a column in it using a transformation function
   *
   * @param columnName the column to replace
   * @param transformation the transformation function
   * @param input The existing DataFrame
   * @return A new DataFrame with the replaced column
   */
  def replaceColumn[I,O] (
    columnName: String,
    columnFcn: I => O
  )(input: DataFrame)(implicit tagI: TypeTag[I], tagO: TypeTag[O]): DataFrame = {
    val tempName = columnName + (new scala.util.Random().nextString(5))
    val newColumn = udf {
      (a: I) => columnFcn(a)
    }(tagO, tagI)(new Column(columnName))

    Pipe(input)
    .to(_.withColumn(tempName, newColumn))
    .to(dropColumns(columnName))
    .to(renameColumns(Map(tempName -> columnName)))
    .run
  }

  /**
   * Cast a set of columns to new types, replacing the original columns
   * @param castMap a Map[String, String] of columnName => datatype
   * @param input The existing DataFrame
   * @return A new DataFrame with the casted columns
   */
  def castColumns(castMap: Map[String, String])(input: DataFrame): DataFrame = {
    // build a map of columns to their original types, then override with castMap
    val originalMap = input.schema.map(f => (f.name, f.dataType.simpleString)).toMap

    val exprs = (originalMap ++ castMap).map(c => {
      s"cast(${c._1} as ${c._2}) as ${c._1}"
    }).toSeq
    input.selectExpr(exprs:_*)
  }

  /**
   * Bring in temporal ops so they can be referred to with dot notation
   */
  val temporal = dataframe.TemporalOps

  /**
   * Bring in numeric ops so they can be referred to with dot notation
   */
  val numeric = dataframe.NumericOps
}
