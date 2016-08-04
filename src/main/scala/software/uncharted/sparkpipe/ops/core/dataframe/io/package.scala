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

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField}

/**
 * Input/output operations for DataFrames, based on the `sparkSession.read` and `DataFrame.write` APIs
 */
package object io {

  /**
   * Create a DataFrame from an input data source
   *
   * @param path A format-specific location String for the source data
   * @param format Specifies the input data source format (parquet by default)
   * @param options A Map[String, String] of options
   * @param schema A StructType schema to apply to the source data
   * @return a DataFrame createad from the specified source
   */
  def read(
    path: String,
    format: String = "parquet",
    options: Map[String, String] = Map[String, String](),
    schema: StructType = new StructType(Array[StructField]())
  )(sparkSession: SparkSession): DataFrame = {
    var reader = sparkSession.read.format(format).options(options)

    if (schema.length > 0) {
      reader = reader.schema(schema)
    }

    if (path.length > 0) {
      reader.load(path)
    } else {
      reader.load()
    }
  }

  /**
   * :: Experimental ::
   * Writes a DataFrame to an output data format/location
   *
   * @param path A format-specific location String for the source data
   * @param format Specifies the output data source format (parquet by default)
   * @param options A Map[String, String] of options
   * @return the input DataFrame, unchanged
   */
  // Can't test because DataFrameWriter is currently marked final
  // $COVERAGE-OFF$
  def write(
    path: String,
    format: String = "parquet",
    options: Map[String, String] = Map[String, String]()
  )(input: DataFrame): DataFrame = {
    if (path.length > 0) {
      input.write.format(format).options(options).save(path)
    } else {
      input.write.format(format).options(options).save()
    }
    input
  }
  // $COVERAGE-ON$
}
