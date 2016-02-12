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
package software.uncharted.sparkpipe.ops.core.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Input/output operations for RDDs, based on the `SparkContext.textFile` API
  */
package object io {
  /**
    * Translates a SQLContext into a SparkContext, so that RDD operations can be called with either
    *
    * @param sqlc A SQLContext in which to run operations
    * @return The spark context from which the SQL context was created
    */
  implicit def mutateContext (sqlc: SQLContext): SparkContext = sqlc.sparkContext

  /**
    * Reads a file into an RDD
    *
    * @param path The location of the source data
    * @param sc The spark context in which to read the data
    * @return An RDD of the text of the source data, line by line
    */
  def read(path: String)(sc: SparkContext): RDD[String] =
    sc.textFile(path)
}
