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
  /** Simple text format, one line per record. */
  val TEXT_FORMAT = "text"
  /** An option key with which to specify the minimum number of partitions into which to read an input file */
  val MIN_PARTITIONS = "minPartitions"
  /** An option key with which to specify the compression codec with which to write text data */
  val CODEC = "codec"
  /** Codec value indicating that a text file should be written using the BZip2 codec */
  val BZIP2_CODEC = "bzip2"
  /** Codec value indicating that a text file should be written using the GZip codec */
  val GZIP_CODEC = "gzip"

  /**
    * Translates a SQLContext into a SparkContext, so that RDD operations can be called with either
    *
    * @param sqlc A SQLContext in which to run operations
    * @return The spark context from which the SQL context was created
    */
  implicit def mutateContext (sqlc: SQLContext): SparkContext = sqlc.sparkContext

  /**
    * Traslate a function from a SparkContext into a function from a SQLContext, so that RDD operations
    * can be run off a Pipe[SQLContext]
    *
    * @param fcn The SparkContext-based function
    * @tparam T The return type of the function
    * @return The same function, but working on a SQLContext.
    */
  implicit def mutateContextFcn[T](fcn: SparkContext => T): SQLContext => T =
    input => fcn(input.sparkContext)

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
    format: String = TEXT_FORMAT,
    options: Map[String, String] = Map[String, String]()
  )(sc: SparkContext): RDD[String] = {
    assert(TEXT_FORMAT == format, "Only text format currently supported")
    if (options.contains(MIN_PARTITIONS)) {
      sc.textFile(path, options(MIN_PARTITIONS).trim.toInt)
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
    format: String = TEXT_FORMAT,
    options: Map[String, String] = Map[String, String]()
  )(input: RDD[T]): RDD[T] = {
    assert(TEXT_FORMAT == format, "Only text format currently supported")
    options.get(CODEC).map(_.trim.toLowerCase) match {
      case Some(BZIP2_CODEC) =>
        input.saveAsTextFile(path, classOf[BZip2Codec])
      case Some(GZIP_CODEC) =>
        input.saveAsTextFile(path, classOf[GzipCodec])
      case _ =>
        input.saveAsTextFile(path)
    }
    input
  }
}
