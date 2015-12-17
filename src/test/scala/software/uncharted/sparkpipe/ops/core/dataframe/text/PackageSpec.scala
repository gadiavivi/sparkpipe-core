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

package software.uncharted.sparkpipe.ops.core.dataframe.text

import org.scalatest._
import software.uncharted.sparkpipe.Spark
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.rdd.toDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, ArrayType}

import scala.collection.mutable.WrappedArray

class PackageSpec extends FunSpec {
  describe("ops.core.dataframe.text") {
    val rdd = Spark.sc.parallelize(Seq(
      (1, "lorem ipsum"),
      (2, "lorem ipsum dolor"),
      (3, "lorem ipsum dolor sit"),
      (4, "lorem ipsum dolor sit amet")
    ))
    val df = toDF(Spark.sqlContext)(rdd)

    describe("#split()") {
      it("should split the specified column on whitespace") {
        val df2 = split("_2")(df)
        assert(df2.schema(1).dataType.equals(ArrayType(StringType, true)))
        val result = df2.select("_2").collect.map(_.apply(0).asInstanceOf[WrappedArray[String]])
        assert(result(0).length == 2)
        assert(result(0)(1).equals("ipsum"))
        assert(result(1).length == 3)
        assert(result(2).length == 4)
        assert(result(2)(3).equals("sit"))
        assert(result(3).length == 5)
      }

      it("should split the specified column on a custom delimiter") {
        val df2 = split("_2", "i")(df)
        assert(df2.schema(1).dataType.equals(ArrayType(StringType, true)))
        val result = df2.select("_2").collect.map(_.apply(0).asInstanceOf[WrappedArray[String]])
        assert(result(0).length == 2)
        assert(result(0)(0).equals("lorem "))
        assert(result(1).length == 2)
        assert(result(2).length == 3)
        assert(result(2)(2).equals("t"))
        assert(result(3).length == 3)
      }
    }

    describe("#stopWordFilter()") {
      it("should remove stop words from the specified column in the input DataFrame") {
        val result = Pipe(df)
                    .to(split("_2"))
                    .to(stopWordFilter("_2", Set("lorem", "ipsum")))
                    .to(_.select("_2").collect)
                    .to(_.map(_.apply(0).asInstanceOf[WrappedArray[String]]))
                    .to(_.map(_.mkString(" ")))
                    .run
        assert(result(0).equals(""))
        assert(result(1).equals("dolor"))
        assert(result(2).equals("dolor sit"))
        assert(result(3).equals("dolor sit amet"))
      }
    }
  }
}
