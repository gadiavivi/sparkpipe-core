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

package software.uncharted.sparkpipe.ops.core.dataframe.temporal

import org.scalatest._
import software.uncharted.sparkpipe.Spark
import software.uncharted.sparkpipe.ops.core.rdd.toDF

import java.text.SimpleDateFormat
import java.sql.Timestamp

class PackageSpec extends FunSpec {
  describe("ops.core.dataframe.temporal") {
    val rdd = Spark.sc.parallelize(Seq(
      (new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-18").getTime), "2015-11-18", 1),
      (new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-19").getTime), "2015-11-19", 2),
      (new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-20").getTime), "2015-11-20", 3),
      (new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-21").getTime), "2015-11-21", 4)
    ))
    val df = toDF(Spark.sparkSession)(rdd)

    describe("#dateFilter()") {
      it("should support filtering rows in an input DataFrame with a String timetamp column, based on a date range") {
        val df2 = dateFilter(
          new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-19"),
          new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-21"),
          "yyyy-MM-dd",
          "_2"
        )(df)
        assert(df2.count == 3)
      }

      it("should support filtering rows in an input DataFrame with a String timetamp column, based on a date range, specified using strings") {
        val df2 = dateFilter(
          "2015-11-19",
          "2015-11-20",
          "yyyy-MM-dd",
          "_2"
        )(df)
        assert(df2.count == 2)
      }

      it("should support filtering rows in an input DataFrame with a Timestamp timestamp column, based on a date range") {
        val df2 = dateFilter(
          new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-17"),
          new SimpleDateFormat("yyyy-MM-dd").parse("2015-11-18"),
          "_1"
        )(df)
        assert(df2.count == 1)
      }
    }

    describe("#parseDate()") {
      it("should facilitate converting a string timestamp column into a TimestampType and adding it as a new column") {
        val df2 = parseDate("_2", "new", "yyyy-MM-dd")(df)
        assert(df2.filter("new = _1").count == df.count)
        assert(df2.schema.size == df.schema.size+1)
      }
    }

    describe("#dateField()") {
      it("should facilitate extracting a single field from a Timestamp column, and placing it a new column") {
        val df2 = dateField("_1", "new", java.util.Calendar.YEAR)(df)
        assert(df2.filter("new = 2015").count == df.count)
        assert(df2.schema.size == df.schema.size+1)
      }
    }
  }
}
