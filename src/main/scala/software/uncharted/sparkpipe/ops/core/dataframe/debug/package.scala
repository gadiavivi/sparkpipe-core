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

import org.apache.spark.sql.{DataFrame, Row}


/*
 * Common operations to aid in debugging dataframe processing in pipelines
 */
package object debug {

  // $COVERAGE-OFF$
  val defaultOutput = (s: String) => println(s)  // scalastyle:off regex
  // $COVERAGE-ON$

  /**
    * Prints the number of rows in a dataframe.
    * @param tag Tag to prepend to output.
    * @param output Optional function to output debug message. Prints to console by default.
    * @param data DataFrame to count.
    * @return The input DataFrame.
    */
  def countDFRows (tag: String = "",
                   output: (String) => Unit = defaultOutput)
                  (data: DataFrame): DataFrame = {
    output(s"[$tag] Number of rows: ${data.count}")
    data
  }

  /**
    * Prints the first n elements of a dataframe.
    * @param rows Number of rows to print.
    * @param tag Tag to prepend to output.
    * @param output Optional function to output debug message. Prints to console by default.
    * @param data Dataframe to extract rows from.
    * @return The input dataFrame.
    */
  def takeDFRows (rows: Int, tag: String = "", output: (String) => Unit = defaultOutput)
                 (data: DataFrame): DataFrame = {
    output(s"[$tag] First $rows rows")
    data.take(rows).zipWithIndex.foreach { case (text, row) => output(s"$row: $text") }
    data
  }

  /**
    * Applies a caller supplied debug function to the first n elements of a dataframe.
    * @param rows Number of rows to process.
    * @param fcn Function to apply to extracted rows.
    * @param data Dataframe to extract rows from.
    * @return The input dataframe.
    */
  def debugDFRows (rows: Int, fcn: Seq[Row] => Unit)(data: DataFrame): DataFrame = {
    fcn(data.take(rows))
    data
  }
}
