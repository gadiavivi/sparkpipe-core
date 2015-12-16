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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row, Column}
import org.apache.spark.sql.functions.{udf, callUDF}
import org.apache.spark.sql.types.{DataType, TimestampType, IntegerType}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

package object temporal {
  /**
   * Pipeline op to filter records to a specific date range.
   *
   * @param minDate Start date for the range.
   * @param maxDate End date for the range.
   * @param format Date parsing string, expressed according to java.text.SimpleDateFormat.
   * @param timeCol Column spec denoting name of time column in input DataFrame.  Column is expected
   *                to be a string.
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, where records outside the specified time range have been removed.
   */
  def dateFilter(minDate: Date, maxDate: Date, format: String, timeCol: String)(input: DataFrame): DataFrame = {
    val formatter = new SimpleDateFormat(format)
    val minTime = minDate.getTime
    val maxTime = maxDate.getTime

    val filterFcn = udf((value: String) => {
                          val time = formatter.parse(value).getTime
                          minTime <= time && time <= maxTime
                        })
    input.filter(filterFcn(new Column(timeCol)))
  }

  /**
   * Pipeline op to filter records to a specific date range.
   *
   * @param minDate Start date for the range, expressed in a format parsable by java.text.SimpleDateFormat.
   * @param maxDate End date for the range, expressed in a format parsable by java.text.SimpleDateFormat.
   * @param format Date parsing string, expressed according to java.text.SimpleDateFormat.
   * @param timeCol Column spec denoting name of time column in input DataFrame.
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, where records outside the specified time range have been removed.
   */
  def dateFilter(minDate: String, maxDate: String, format: String, timeCol: String)(input: DataFrame): DataFrame = {
    val formatter = new SimpleDateFormat(format)
    val minTime = new Date(formatter.parse(minDate).getTime)
    val maxTime = new Date(formatter.parse(maxDate).getTime)
    dateFilter(minTime, maxTime, format, timeCol)(input)
  }

  /**
   * Pipeline op to filter records to a specific date range.
   *
   * @param minDate Start date for the range.
   * @param maxDate End date for the range.
   * @param timeCol Column spec denoting name of time column in input DataFrame.  In this case time column
   *                is expected to store a Date.
   * @param input Input pipeline data to filter.
   * @return Transformed pipeline data, where records outside the specified time range have been removed.
   */
  def dateFilter(minDate: Date, maxDate: Date, timeCol: String)(input: DataFrame): DataFrame = {
    val minTime = minDate.getTime
    val maxTime = maxDate.getTime
    val filterFcn = udf((time: Timestamp) => {
                          minTime <= time.getTime && time.getTime <= maxTime
                        })
    input.filter(filterFcn(new Column(timeCol)))
  }

  /**
   * Pipeline op to parse a string date column into a timestamp column
   * @param stringDateCol The column from which to get the date (as a string)
   * @param dateCol The column into which to put the date (as a timestamp)
   * @param format The expected format of the date
   * @param input Input pipeline data to transform
   * @return Transformed pipeline data with the new time field column.
   */
  def parseDate(stringDateCol: String, dateCol: String, format: String)(input: DataFrame): DataFrame = {
    val formatter = new SimpleDateFormat(format);
    val fieldExtractor: String => Timestamp = i => {
      val date = formatter.parse(i)
      new Timestamp(date.getTime)
    }

    addColumn(dateCol, fieldExtractor, stringDateCol)(input)
  }

  /**
   * Pipeline op to get a single field out of a date, and create a new column with that field
   *
   * For instance, this can take a date, and transform it to a week of the year, or a day of the month.
   *
   * @param timeCol Column spec denoting the name of a time column in the input DataFrame.  In this case,
   *                the column is expected to store a Date.
   * @param fieldCol The name of the column to create with the time field value
   * @param timeField The field of the date to retrieve
   * @param input Input pipeline data to transform
   * @return Transformed pipeline data with the new time field column.
   */
  def dateField(timeCol: String, fieldCol: String, timeField: Int)(input: DataFrame): DataFrame = {
    val fieldExtractor: Date => Int = date => {
      val calendar = new GregorianCalendar()
      calendar.setTime(date)
      calendar.get(timeField)
    }
    addColumn(fieldCol, fieldExtractor, timeCol)(input)
  }
}
