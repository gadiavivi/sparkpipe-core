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
package software.uncharted.sparkpipe.ops.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


/*
 * Common operations to aid in debugging RDD processing in pipelines
 */
package object debug {
  // $COVERAGE-OFF$
  val defaultOutput = (s: String) => println(s) // scalastyle:off regex
  // $COVERAGE-ON$

  /**
    * Prints the number of rows in an RDD.
    * @param tag Tag to prepend to output.
    * @param output Optional function to output debug message. Prints to console by default.
    * @param rdd RDD to count.
    * @return The input RDD.
    */
  def countRDDRows[T] (tag: String = "",
                   output: (String) => Unit = defaultOutput)
                  (rdd: RDD[T]): RDD[T] = {
    output(s"[$tag] Number of rows: ${rdd.count}")
    rdd
  }

  /**
    * Prints the first n elements of an RDD.
    * @param rows Number of rows to print.
    * @param tag Tag to prepend to output.
    * @param output Optional function to output debug message. Prints to console by default.
    * @param rdd RDD to extract rows from.
    * @return The input RDD.
    */
  def takeRDDRows[T](rows: Int, tag: String = "", output: (String) => Unit = defaultOutput)
                 (rdd: RDD[T]): RDD[T] = {
    output(s"[$tag] First $rows rows")
    rdd.take(rows).zipWithIndex.foreach { case (text, row) => output(s"$row: $text") }
    rdd
  }

  /**
    * Applies a caller supplied debug function to the first n elements of an RDD.
    * @param rows Number of rows to process.
    * @param fcn Function to apply to extracted rows.
    * @param rdd RDD to extract rows from.
    * @return The input RDD.
    */
  def debugRDDRows[T] (rows: Int, fcn: Seq[T]=> Unit)(rdd: RDD[T]): RDD[T] = {
    fcn(rdd.take(rows))
    rdd
  }
}
