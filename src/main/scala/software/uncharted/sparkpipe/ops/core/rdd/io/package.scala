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

import org.apache.hadoop.io.compress.{GzipCodec, BZip2Codec}
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
    * @param format The format in which to read the data.  Currently, only "text" is supported.
    * @param options A Map[String, String] of options.  Currently, the only supported option is "minPartitions", which
    *                will set the minimum number of partitions into which the data is read.
    * @param sc The spark context in which to read the data
    * @return An RDD of the text of the source data, line by line
    */
  def read(
    path: String,
    format: String = "text",
    options: Map[String, String] = Map[String, String]()
  )(sc: SparkContext): RDD[String] = {
    assert("text" == format, "Only text format currently supported")
    if (options.contains("minPartitions")) {
      sc.textFile(path, options("minPartitions").trim.toInt)
    } else {
      sc.textFile(path)
    }
  }

  /**
    * Write an RDD
    * @param path The location to which to write the data
    * @param format The format in which to write the data.  Currently, only "text" is supported.
    * @param options A Map[String, String] of options.  Currently, only the "codec" option is supported, for which
    *                valid values are "bzip2", and "gzip"; any other value will result in the default codec.
    * @param input The RDD to write
    * @tparam T The type of data contained in the RDD
    * @return The input RDD
    */
  def write[T] (
    path: String,
    format: String = "text",
    options: Map[String, String] = Map[String, String]()
  )(input: RDD[T]): RDD[T] = {
    assert("text" == format, "Only text format currently supported")
    options.get("codec").map(_.trim.toLowerCase) match {
      case Some("bzip2") =>
        input.saveAsTextFile(path, classOf[BZip2Codec])
      case Some("gzip") =>
        input.saveAsTextFile(path, classOf[GzipCodec])
      case _ =>
        input.saveAsTextFile(path)
    }
    input
  }
}
