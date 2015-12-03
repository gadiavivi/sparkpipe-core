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
import org.apache.spark.storage.StorageLevel
import software.uncharted.sparkpipe.Spark
import software.uncharted.sparkpipe.ops.core.rdd.toDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{sum}
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._

class DataFrameOpsSpec extends FunSpec with MockitoSugar {
  describe("ops.core.dataframe") {
    val rdd = Spark.sc.parallelize(Seq(
      (0, 1, 2, 3, 4),
      (1, 2, 3, 4, 5),
      (2, 3, 4, 5, 6),
      (3, 4, 5, 6, 7)
    ))
    val df = toDF(Spark.sqlContext)(rdd)

    describe("#toRDD()") {
      it("should convert an input DataFrame to an RDD[Row]") {
        val rdd2 = toRDD(df)
        assert(rdd2.isInstanceOf[RDD[Row]])
      }
    }

    describe("#cache()") {
      it("should call .cache() on an input DataFrame") {
        val mockDf = mock[DataFrame]
        val s = spy(df)
        cache(s)
        verify(s).cache()
      }
    }

    describe("#dropColumns()") {
      it("should remove the specified column from an input DataFrame") {
        val df2 = dropColumns("_1")(df)
        assert(df2.schema.size == df.schema.size-1)
        assert(df2.first.getInt(0).equals(1))
      }

      it("should be a no-op when the specified column does not exist in the input DataFrame") {
        val df2 = dropColumns("col")(df)
        assert(df.schema.size == df2.schema.size)
      }

      it("should allow the removal of multiple columns from an input DataFrame") {
        val df2 = dropColumns("_1", "_3")(df)
        assert(df2.schema.size == df.schema.size-2)
        assert(df2.first.getInt(0).equals(1))
        assert(df2.first.getInt(1).equals(3))
      }
    }

    describe("#renameColumns()") {
      it("should support renaming columns in an input DataFrame") {
        val df2 = renameColumns(Map("_1" -> "new_1", "_3" -> "new_3"))(df)
        assert(df2.schema.size == df.schema.size)
        assert(df2.schema(0).dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.schema(0).name.equals("new_1"))
        assert(df2.first.getInt(0) == df.first.getInt(0))
        assert(df2.schema(2).dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.schema(2).name.equals("new_3"))
        assert(df2.first.getInt(2) == df.first.getInt(2))
        assert(df2.schema(1).name.equals("_2"))
        assert(df2.schema(3).name.equals("_4"))
      }

      it("should be a no-op when the specified column does not exist in the input DataFrame") {
        val df2 = renameColumns(Map("col" -> "new", "_3" -> "new_3"))(df)
        assert(df.schema.size == df2.schema.size)
        assert(df2.schema(0).name.equals("_1"))
        assert(df2.schema(1).name.equals("_2"))
        assert(df2.schema(2).name.equals("new_3"))
        assert(df2.schema(3).name.equals("_4"))
        assert(df2.schema(4).name.equals("_5"))
      }
    }

    describe("#addColumn()") {
      it("should support adding a column to a DataFrame") {
        val df2 = addColumn(
          "new",
          (i: Array[Any]) => 8
        )(df)
        assert(df2.schema.size == df.schema.size + 1)
        assert(df2.schema("new").dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.filter("new = 8").count == df.count)
      }

      it("should support adding a column to a DataFrame based on an input column") {
        val df2 = addColumn(
          "new",
          (i: Array[Any]) => i(0).asInstanceOf[Int] + 1,
          "_1"
        )(df)
        assert(df2.schema.size == df.schema.size + 1)
        assert(df2.schema("new").dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.filter("new = _1+1").count == df.count)
      }

      it("should support adding a column to a DataFrame based on two input columns") {
        val df2 = addColumn(
          "new",
          (i: Array[Any]) => i(0).asInstanceOf[Int] + i(1).asInstanceOf[Int],
          "_1", "_2"
        )(df)
        assert(df2.schema.size == df.schema.size + 1)
        assert(df2.schema("new").dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.filter("new = _1+_2").count == df.count)
      }

      it("should support adding a column to a DataFrame based on three input columns") {
        val df2 = addColumn(
          "new",
          (i: Array[Any]) => i(0).asInstanceOf[Int] + i(1).asInstanceOf[Int] + i(2).asInstanceOf[Int],
          "_1", "_2", "_3"
        )(df)
        assert(df2.schema.size == df.schema.size + 1)
        assert(df2.schema("new").dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.filter("new = _1+_2+_3").count == df.count)
      }

      it("should support adding a column to a DataFrame based on four input columns") {
        val df2 = addColumn(
          "new",
          (i: Array[Any]) => i(0).asInstanceOf[Int] + i(1).asInstanceOf[Int] + i(2).asInstanceOf[Int] + i(3).asInstanceOf[Int],
          "_1", "_2", "_3", "_4"
        )(df)
        assert(df2.schema.size == df.schema.size + 1)
        assert(df2.schema("new").dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.filter("new = _1+_2+_3+_4").count == df.count)
      }

      it("should support adding a column to a DataFrame based on five input columns") {
        val df2 = addColumn(
          "new",
          (i: Array[Any]) => i(0).asInstanceOf[Int] + i(1).asInstanceOf[Int] + i(2).asInstanceOf[Int] + i(3).asInstanceOf[Int] + i(4).asInstanceOf[Int],
          "_1", "_2", "_3", "_4", "_5"
        )(df)
        assert(df2.schema.size == df.schema.size + 1)
        assert(df2.schema("new").dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df2.filter("new = _1+_2+_3+_4+_5").count == df.count)
      }
    }

    describe("replaceColumn()") {
      it("should support replacing a column in a DataFrame based on a transformation function") {
        val df2 = replaceColumn(
          "_1",
          (i: Int) => i.asInstanceOf[Double] + 1D
        )(df)
        assert(df2.schema.size == df.schema.size)
        assert(df2.schema("_1").dataType.equals(org.apache.spark.sql.types.DoubleType))
        assert(df2.agg(sum(df2("_1"))).first()(0).equals(10D))
      }
    }

    describe("castColumns()") {
      it ("should support casting of columns in a DataFrame using a Map from columnName to desired type") {
        val df2 = castColumns(
          Map("_1" -> "double", "_2" -> "float", "_3" -> "string")
        )(df)
        assert(df.schema.size == df.schema.size)
        assert(df2.schema("_1").dataType.equals(org.apache.spark.sql.types.DoubleType))
        assert(df2.schema("_2").dataType.equals(org.apache.spark.sql.types.FloatType))
        assert(df2.schema("_3").dataType.equals(org.apache.spark.sql.types.StringType))
      }
    }

    describe(".temporal") {
      it("should make temporal operations available via .temporal") {
        assert(temporal == TemporalOps)
      }
    }

    describe(".numeric") {
      it("should make numeric operations available via .numeric") {
        assert(numeric == NumericOps)
      }
    }
  }
}
