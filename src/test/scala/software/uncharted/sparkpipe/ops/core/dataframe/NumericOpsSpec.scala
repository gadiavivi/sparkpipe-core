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

import org.scalatest._
import software.uncharted.sparkpipe.Spark
import software.uncharted.sparkpipe.ops.core.rdd.toDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{FloatType, DoubleType, IntegerType, LongType, TimestampType, DateType, StringType, StructType, StructField}
import java.sql.{Date, Timestamp}

class NumericOpsSpec extends FunSpec {
  describe("NumericOps") {
    val rdd = Spark.sc.parallelize(Seq(
      (new Timestamp(new java.util.Date().getTime), new Date(new java.util.Date().getTime), 1, 1D, 1F, 1L, "1"),
      (new Timestamp(new java.util.Date().getTime), new Date(new java.util.Date().getTime), 2, 2D, 2F, 2L, "2"),
      (new Timestamp(new java.util.Date().getTime), new Date(new java.util.Date().getTime), 3, 3D, 3F, 3L, "3"),
      (new Timestamp(new java.util.Date().getTime), new Date(new java.util.Date().getTime), 4, 4D, 4F, 4L, "4")
    ))
    val df = toDF(Spark.sqlContext)(rdd)

    describe("#enumerate()") {
      it("should convert all supported columns into doubles, and drop any unsupported ones") {
        val df2 = NumericOps.enumerate(df)
        assert(df2.schema.filter(_.dataType == DoubleType).length == df.schema.length-1)
      }

      it("should throw an exception if the input DataFrame contains no compatible columns") {
        intercept[IllegalArgumentException] {
          val df2 = NumericOps.enumerate(df.select("_7"))
        }
      }
    }

    describe("#summaryStats()") {
      it("should produce summary statistics for numeric-compatible columns in a source DataFrame, dropping incompatible columns") {
        val rddNull = Spark.sc.parallelize(Seq(Row(null, null, 5, 5D, 5F, null, null)))
        val struct = StructType(
          StructField("_1", TimestampType, true) ::
          StructField("_2", DateType, true) ::
          StructField("_3", IntegerType, true) ::
          StructField("_4", DoubleType, true) ::
          StructField("_5", FloatType, true) ::
          StructField("_6", LongType, true) ::
          StructField("_7", StringType, true) :: Nil
        )
        val dfNull = Spark.sqlContext.createDataFrame(rddNull, struct)
        val dfWithNulls = df.unionAll(dfNull)
        val result = NumericOps.summaryStats(Spark.sc)(dfWithNulls)
        // counts (verify ignoring nulls )
        assert(result(0).count == 4)
        assert(result(1).count == 4)
        assert(result(2).count == 5)
        assert(result(3).count == 5)
        assert(result(4).count == 5)
        assert(result(5).count == 4)
        // mins
        assert(result(2).min == 1)
        assert(result(3).min == 1)
        assert(result(4).min == 1)
        assert(result(5).min == 1)
        // maxes
        assert(result(2).max == 5)
        assert(result(3).max == 5)
        assert(result(4).max == 5)
        assert(result(5).max == 4)
        // means
        assert(result(2).mean == 3)
        assert(result(3).mean == 3)
        assert(result(4).mean == 3)
        assert(result(5).mean == 2.5)
      }
    }
  }
}
