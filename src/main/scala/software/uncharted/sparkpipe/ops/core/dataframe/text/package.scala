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
import software.uncharted.sparkpipe.{ops => ops}

package object text {

  /**
   * Pipeline op to remove stop words from a string column
   *
   * @param stopWords A Set[String] of words to remove
   * @param stringCol Column spec denoting name of string column in input DataFrame
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, with stop words removed from the specified column
   */
  def stopWordFilter(stopWords: Set[String], stringCol: String)(input: DataFrame): DataFrame = {
    val bStopWordsLookup = input.sqlContext.sparkContext.broadcast(
      collection.mutable.LinkedHashSet[String]() ++ stopWords
    )
    val result = ops.core.dataframe.replaceColumn(stringCol, (s: String) => {
      var words = collection.mutable.LinkedHashSet[String]() ++ s.split("\\s+")
      (words -- bStopWordsLookup.value).mkString(" ")
    })(input)

    bStopWordsLookup.unpersist()

    result
  }
}
