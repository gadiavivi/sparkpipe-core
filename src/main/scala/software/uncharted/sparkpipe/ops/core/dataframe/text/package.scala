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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.IndexedSeq
import software.uncharted.sparkpipe.ops
import software.uncharted.sparkpipe.ops.core.dataframe.text.util.UniqueTermAccumulator

import scala.util.matching.Regex
import scala.reflect.runtime.universe.TypeTag

/**
 * Common pipeline operations for dealing with textual data
 */
package object text {

  /**
   * Replaces all occurrences of pattern in a String column with sub
   *
   * @param stringCol the name of a String column in the input DataFrame
   * @param pattern a regular expression
   * @param sub the string to substitute for the pattern
   * @param input Input pipeline data to transform
   * @return Transformed pipeline data, with instances of the given pattern in input replaced with sub
   */
  def replaceAll(stringCol: String, pattern: Regex, sub: String)(input: DataFrame): DataFrame = {
    ops.core.dataframe.replaceColumn(stringCol, (s: String) => {
      try {
        pattern.replaceAllIn(s, sub)
      } catch {
        case _: Throwable => s
      }
    }: String)(input)
  }

  /**
   * Removes all occurrences of pattern in a String column
   *
   * @param stringCol the name of a String column in the input DataFrame
   * @param pattern a regular expression
   * @param input Input pipeline data to transform
   * @return Transformed pipeline data, with instances of the given pattern in input removed
   */
  def removeAll(stringCol: String, pattern: Regex)(input: DataFrame): DataFrame = {
    replaceAll(stringCol, pattern, "")(input)
  }

  /**
   * Splits a String column into an Array[String] column using a delimiter
   * (whitespace, by default)
   *
   * @param stringCol the name of a String column in the input DataFrame
   * @param delimiter a delimiter to split the String column on
   * @param input Input pipeline data to transform
   * @return Transformed pipeline data, with the given string column split on the delimiter
   */
  def split(stringCol: String, delimiter: String = "\\s+")(input: DataFrame): DataFrame = {
    ops.core.dataframe.replaceColumn(stringCol, (s: String) => {
      try {
        s.split(delimiter)
      } catch {
        case _: Throwable => Array[String]()
      }
    }: Array[String])(input)
  }

  /**
   * Apply a transformation to every String in an Array[String] column.
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param mapFcn A transformation function String => O
   * @param input Input pipeline data to transform
   * @return Transformed pipeline data, with the mapFcn applied to every term in every row of the Array[String] column
   */
  def mapTerms[O](arrayCol: String, mapFcn: String => O)(input: DataFrame)(implicit tag: TypeTag[O]): DataFrame = {
    ops.core.dataframe.replaceColumn(arrayCol, (s: IndexedSeq[String]) => {
      s.map(mapFcn)
    }: IndexedSeq[O])(input)
  }

  /**
   * Pipeline op to remove stop words from a string column
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param stopTerms A Set[String] of words to remove
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, with stop words removed from the specified column
   */
  def stopTermFilter(arrayCol: String, stopTerms: Set[String])(input: DataFrame): DataFrame = {
    val bStopTermsLookup = input.sparkSession.sparkContext.broadcast(
      collection.mutable.LinkedHashSet[String]() ++ stopTerms
    )

    val result = ops.core.dataframe.replaceColumn(arrayCol, (s: IndexedSeq[String]) => {
      s.filterNot(w => bStopTermsLookup.value.contains(w))
    })(input)

    bStopTermsLookup.unpersist()

    result
  }

  /**
   * Pipeline op to remove stop patterns from a string column
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param stopPattern A Regex pattern describing words to remove
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, with matching words removed from the specified column
   */
  def stopTermFilter(arrayCol: String, stopPattern: Regex)(input: DataFrame): DataFrame = {
    val result = ops.core.dataframe.replaceColumn(arrayCol, (s: IndexedSeq[String]) => {
      s.filterNot(w => w match {
        case stopPattern(_*) => true
        case _ =>  false
      })
    })(input)
    result
  }

  /**
   * Pipeline op to filter a string column down to terms of interest
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param includeTerms A Set[String] of words to filter to
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, with the specified column filterd down to terms of interest
   */
  def includeTermFilter(arrayCol: String, includeTerms: Set[String])(input: DataFrame): DataFrame = {
    val bIncludeTermsLookup = input.sparkSession.sparkContext.broadcast(
      collection.mutable.LinkedHashSet[String]() ++ includeTerms
    )

    val result = ops.core.dataframe.replaceColumn(arrayCol, (s: IndexedSeq[String]) => {
      s.filter(w => bIncludeTermsLookup.value.contains(w))
    })(input)

    bIncludeTermsLookup.unpersist()

    result
  }

  /**
   * Pipeline op to filter a string column down to terms which match a certain pattern
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param includePattern A Regex pattern describing words to include
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, with non-matching words removed from the specified column
   */
  def includeTermFilter(arrayCol: String, includePattern: Regex)(input: DataFrame): DataFrame = {
    val result = ops.core.dataframe.replaceColumn(arrayCol, (s: IndexedSeq[String]) => {
      s.filter(w => w match {
        case includePattern(_*) => true
        case _ =>  false
      })
    })(input)
    result
  }

  /**
   * Produces a Map[String,Int] of unique terms from an Array[String] column
   * along with associated counts
   *
   * @param arrayCol The name of an ArrayType(StringType) column in the input DataFrame
   * @param input Input pipeline data to analyze
   * @return the Map[String, Int] of unique terms and their counts
   */
  def uniqueTerms(arrayCol: String)(input: DataFrame): collection.mutable.Map[String, Int] = {
    val accumulator = new UniqueTermAccumulator
    input.sparkSession.sparkContext.register(accumulator)

    input.select(arrayCol).foreach(row => {
      accumulator.add(row(0).asInstanceOf[Seq[String]])
    })

    accumulator.value
  }

  /**
    * Checks for keyword matches in a text field and keeps rows with hits.
    *
    * @param stringCol Column containing text to match against.
    * @param terms Keywords to match.
    * @param input DataFrame from previous stage
    * @param caseSensitive True if matching should be case sensitive, False otherwise.
    * @return Filtered DataFrame
    */
  def includeRowTermFilter(stringCol: String, terms: Seq[String], caseSensitive: Boolean = false)(input: DataFrame): DataFrame = {
    val regexPattern = genTermRegex(terms, caseSensitive)
    rowTermFilter(regexPattern, stringCol, true)(input)
  }

  /**
    * Checks for keyword matches in a text field and removes rows with hits.
    *
    * @param stringCol Column containing text to match against.
    * @param terms Keywords to match.
    * @param input DataFrame from previous stage
    * @param caseSensitive True if matching should be case sensitive, False otherwise.
    * @return Filtered DataFrame
    */
  def stopRowTermFilter(stringCol: String, terms: Seq[String], caseSensitive: Boolean = false)(input: DataFrame): DataFrame = {
    val regexPattern = genTermRegex(terms, caseSensitive)
    rowTermFilter(regexPattern, stringCol, false)(input)
  }

  private def genTermRegex(terms: Seq[String], caseSensitive: Boolean = true) : Regex = {
    // creates a single regex of type (\bdog\b|\bcat\b) for a list of [dog, cat]
    val adjustedKeys = terms.map(k => s"\\b$k\\b")
    adjustedKeys.mkString(if (caseSensitive) "(" else "(?i)(", "|", ")").r
  }

  /**
    * Applies regex matching to text in a field, and includes rows with hits.
    *
    * @param stringCol Column containing text to match against.
    * @param pattern Regex pattern describing text matches.
    * @param input DataFrame from previous stage
    * @return Filtered DataFrame
    */
  def includeRowTermFilter(stringCol: String, pattern: Regex)(input: DataFrame): DataFrame = {
    rowTermFilter(pattern, stringCol, include = true)(input)
  }

  /**
    * Applies regex matching to text in a field, and excludes rows with hits.
    *
    * @param stringCol Column containing text to match against.
    * @param pattern Regex pattern describing text matches.
    * @param input DataFrame from previous stage
    * @return Filtered DataFrame
    */
  def stopRowTermFilter(stringCol: String, pattern: Regex)(input: DataFrame): DataFrame = {
    rowTermFilter(pattern, stringCol, include = false)(input)
  }

  private def rowTermFilter(pattern: Regex, stringCol: String, include: Boolean)(input: DataFrame): DataFrame = {
    val broadcastPattern = input.sqlContext.sparkContext.broadcast(pattern)
    val filterFunc = udf((text: String) => {
      Option(text).flatMap { t =>
        broadcastPattern.value.findFirstIn(text)
      }.isDefined
    })
    if (include) {
      input.filter(filterFunc(new Column(stringCol)))
    } else {
      input.filter(!filterFunc(new Column(stringCol)))
    }
  }
}
