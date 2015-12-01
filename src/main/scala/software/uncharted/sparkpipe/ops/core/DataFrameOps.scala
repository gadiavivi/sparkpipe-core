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
import org.apache.spark.sql.functions.{udf, callUDF}
import org.apache.spark.sql.types.DataType
import scala.reflect.runtime.universe.{TypeTag, typeTag}

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
  }

  /**
   * Remove columns from a DataFrame
   * @param colNames the named columns to remove
   * @param input the input DataFrame
   * @return the resultant DataFrame, without the specified column
   */
  def dropColumn(colNames: String*)(input: DataFrame): DataFrame = {
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
  def renameColumn(nameMap: Map[String, String])(input: DataFrame): DataFrame = {
    var cur = input;
    nameMap.foreach(m => {
      cur = cur.withColumnRenamed(m._1, m._2)
    })
    cur
  }

  /**
   * Take an existing DataFrame, and add a new column to it.
   * @param columnName The name of the column to add
   * @param columnFcn A function mapping the values of the base data specified by inputColumns onto an output value
   * @param inputColumns The input columns needed to calculate the output column; their extracted values become the
   *                     inputs to columnFcn
   * @param input The existing DataFrame
   * @return A new DataFrame with the named added value.
   */
  def addColumn[T] (
    columnName: String,
    columnFcn: Array[Any] => T,
    inputColumns: String*
  )(input: DataFrame)(implicit tag: TypeTag[T]): DataFrame = {
    val columns = inputColumns.map(new Column(_))
    val newColumn = inputColumns.length match {
      case  0 => udf {
        () => columnFcn(Array[Any]())
      }(tag)()
      case  1 => udf {
        (a: Any) => columnFcn(Array[Any](a))
      }(tag, typeTag[Any])(columns(0))
      case  2 => udf {
        (a: Any, b: Any) => columnFcn(Array[Any](a,b))
      }(tag, typeTag[Any], typeTag[Any])(columns(0), columns(1))
      case  3 => udf {
        (a: Any, b: Any, c: Any) => columnFcn(Array[Any](a,b,c))
      }(tag, typeTag[Any], typeTag[Any], typeTag[Any])(columns(0), columns(1), columns(2))
      case  4 => udf {
        (a: Any, b: Any, c: Any, d: Any) => columnFcn(Array[Any](a,b,c,d))
      }(tag, typeTag[Any], typeTag[Any], typeTag[Any], typeTag[Any])(columns(0), columns(1), columns(2), columns(3))
      case  5 => udf {
        (a: Any, b: Any, c: Any, d: Any, e: Any) => columnFcn(Array[Any](a, b, c, d, e))
      }(tag, typeTag[Any], typeTag[Any], typeTag[Any], typeTag[Any], typeTag[Any])(columns(0), columns(1), columns(2), columns(3), columns(4))
    }
    input.withColumn(columnName, newColumn)
  }

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
    .to(dropColumn(columnName))
    .to(renameColumn(Map(tempName -> columnName)))
    .run
  }

  // TODO shortcut cast operation takes a Map[colName -> destinationType], replace columns with original names

  /**
   * Bring in temporal ops so they can be referred to with dot notation
   */
  val temporal = dataframe.TemporalOps

  /**
   * Bring in numeric ops so they can be referred to with dot notation
   */
  val numeric = dataframe.NumericOps
}
