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

import org.apache.spark.sql.DataFrame
import scala.collection.mutable.IndexedSeq
import software.uncharted.sparkpipe.{ops => ops}

package object text {

  /**
   * Splits a String column into an Array[String] column using a delimiter
   * (whitespace, by default)
   *
   * @param stringcol the name of a String column in the input DataFrame
   * @param delimiter a delimiter to split the sString column on
   * @return Transformed pipeline data, with the given string column split on the delimiter
   */
  def split(stringCol: String, delimiter: String = "\\s+")(input: DataFrame): DataFrame = {
    ops.core.dataframe.replaceColumn(stringCol, (s: String) => {
      s.split(delimiter)
    }: Array[String])(input)
  }

  /**
   * Pipeline op to remove stop words from a string column
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param stopWords A Set[String] of words to remove
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, with stop words removed from the specified column
   */
  def stopWordFilter(arrayCol: String, stopWords: Set[String])(input: DataFrame): DataFrame = {
    val bStopWordsLookup = input.sqlContext.sparkContext.broadcast(
      collection.mutable.LinkedHashSet[String]() ++ stopWords
    )

    val result = ops.core.dataframe.replaceColumn(arrayCol, (s: IndexedSeq[String]) => {
      s.filterNot(w => bStopWordsLookup.value.contains(w))
    })(input)

    bStopWordsLookup.unpersist()

    result
  }

  /**
   * Pipeline op to filter a string column down to terms of interest
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param includeWords A Set[String] of words to filter to
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, with the specified column filterd down to terms of interest
   */
  def includeWordFilter(arrayCol: String, includeWords: Set[String])(input: DataFrame): DataFrame = {
    val bIncludeWordsLookup = input.sqlContext.sparkContext.broadcast(
      collection.mutable.LinkedHashSet[String]() ++ includeWords
    )

    val result = ops.core.dataframe.replaceColumn(arrayCol, (s: IndexedSeq[String]) => {
      s.filter(w => bIncludeWordsLookup.value.contains(w))
    })(input)

    bIncludeWordsLookup.unpersist()

    result
  }
}
