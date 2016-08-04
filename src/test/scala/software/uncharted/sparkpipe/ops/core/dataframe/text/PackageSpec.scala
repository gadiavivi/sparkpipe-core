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

package software.uncharted.sparkpipe.ops.core.dataframe.text

import org.scalatest._
import software.uncharted.sparkpipe.Spark
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops.core.rdd.toDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, ArrayType}

import scala.collection.mutable.IndexedSeq

class PackageSpec extends FunSpec {
  describe("ops.core.dataframe.text") {
    val rdd = Spark.sc.parallelize(Seq(
      (1, "lorem ipsum"),
      (2, "lorem ipsum dolor"),
      (3, "lorem ipsum dolor sit"),
      (4, "lorem ipsum dolor sit amet lorem"),
      (5, null)
    ))
    val df = toDF(Spark.sparkSession)(rdd)

    describe("#replaceAll()") {
      it("should replace all instances of the given pattern within a string column with a substitution string") {
        val df2 = replaceAll("_2", "lorem".r, "merol")(df)
        assert(df2.schema(1).dataType.equals(StringType))
        val result = df2.select("_2").collect
        assert(result(0)(0).equals("merol ipsum"))
        assert(result(1)(0).equals("merol ipsum dolor"))
        assert(result(2)(0).equals("merol ipsum dolor sit"))
        assert(result(3)(0).equals("merol ipsum dolor sit amet merol"))
      }
    }

    describe("#removeAll()") {
      it("should remove all instances of the given pattern within a string column") {
        val df2 = removeAll("_2", "lorem".r)(df)
        assert(df2.schema(1).dataType.equals(StringType))
        val result = df2.select("_2").collect
        assert(result(0)(0).equals(" ipsum"))
        assert(result(1)(0).equals(" ipsum dolor"))
        assert(result(2)(0).equals(" ipsum dolor sit"))
        assert(result(3)(0).equals(" ipsum dolor sit amet "))
      }
    }

    describe("#split()") {
      it("should split the specified column on whitespace") {
        val df2 = split("_2")(df)
        assert(df2.schema(1).dataType.equals(ArrayType(StringType, true)))
        val result = df2.select("_2").collect.map(_.apply(0).asInstanceOf[IndexedSeq[String]])
        assert(result(0).length == 2)
        assert(result(0)(1).equals("ipsum"))
        assert(result(1).length == 3)
        assert(result(2).length == 4)
        assert(result(2)(3).equals("sit"))
        assert(result(3).length == 6)
        assert(result(4).length == 0)
      }

      it("should split the specified column on a custom delimiter") {
        val df2 = split("_2", "i")(df)
        assert(df2.schema(1).dataType.equals(ArrayType(StringType, true)))
        val result = df2.select("_2").collect.map(_.apply(0).asInstanceOf[IndexedSeq[String]])
        assert(result(0).length == 2)
        assert(result(0)(0).equals("lorem "))
        assert(result(1).length == 2)
        assert(result(2).length == 3)
        assert(result(2)(2).equals("t"))
        assert(result(3).length == 3)
      }
    }

    describe("#mapTerms()") {
      it("should apply a map function to every term in an Array[String] column") {
        val result = Pipe(df)
                     .to(split("_2"))
                     .to(mapTerms("_2", (s: String) => s.length))
                     .to(_.select("_2").collect)
                     .to(_.map(_.apply(0).asInstanceOf[IndexedSeq[Int]]))
                     .to(_.map(_.mkString(" ")))
                     .run
        assert(result(0).equals("5 5"))
        assert(result(1).equals("5 5 5"))
        assert(result(2).equals("5 5 5 3"))
        assert(result(3).equals("5 5 5 3 4 5"))
      }
    }

    describe("#stopTermFilter()") {
      it("should remove stop words from the specified column in the input DataFrame") {
        val result = Pipe(df)
                    .to(split("_2"))
                    .to(stopTermFilter("_2", Set("lorem", "ipsum")))
                    .to(_.select("_2").collect)
                    .to(_.map(_.apply(0).asInstanceOf[IndexedSeq[String]]))
                    .to(_.map(_.mkString(" ")))
                    .run
        assert(result(0).equals(""))
        assert(result(1).equals("dolor"))
        assert(result(2).equals("dolor sit"))
        assert(result(3).equals("dolor sit amet"))
      }
      it("should remove stop words matching a pattern from the specified column in the input DataFrame") {
        val result = Pipe(df)
                    .to(split("_2"))
                    .to(stopTermFilter("_2", """\w{5}""".r))
                    .to(_.select("_2").collect)
                    .to(_.map(_.apply(0).asInstanceOf[IndexedSeq[String]]))
                    .to(_.map(_.mkString(" ")))
                    .run
        assert(result(0).equals(""))
        assert(result(1).equals(""))
        assert(result(2).equals("sit"))
        assert(result(3).equals("sit amet"))
      }
    }

    describe("#includeTermFilter()") {
      it("should filter the specified column in the input DataFrame down to only the specified words") {
        val result = Pipe(df)
                    .to(split("_2"))
                    .to(includeTermFilter("_2", Set("lorem", "ipsum")))
                    .to(_.select("_2").collect)
                    .to(_.map(_.apply(0).asInstanceOf[IndexedSeq[String]]))
                    .to(_.map(_.mkString(" ")))
                    .run
        assert(result(0).equals("lorem ipsum"))
        assert(result(1).equals("lorem ipsum"))
        assert(result(2).equals("lorem ipsum"))
        assert(result(3).equals("lorem ipsum lorem"))
      }
      it("should filter the specified column in the input DataFrame down to only words which match the specified pattern") {
        val result = Pipe(df)
                    .to(split("_2"))
                    .to(includeTermFilter("_2", """\w{5}""".r))
                    .to(_.select("_2").collect)
                    .to(_.map(_.apply(0).asInstanceOf[IndexedSeq[String]]))
                    .to(_.map(_.mkString(" ")))
                    .run
        assert(result(0).equals("lorem ipsum"))
        assert(result(1).equals("lorem ipsum dolor"))
        assert(result(2).equals("lorem ipsum dolor"))
        assert(result(3).equals("lorem ipsum dolor lorem"))
      }
    }

    describe("#uniqueTerms()") {
      it("should produce a list of unique terms from an Array[String] column in an input DataFrame") {
        val result = Pipe(df)
                     .to(_.union(dfNull)) //add some nulls so we can test handling them
                     .to(split("_2"))
                     .to(uniqueTerms("_2"))
                     .run
        assert(result.size == 5)
        assert(result.getOrElse("lorem", 0) == 5)
        assert(result.getOrElse("ipsum", 0) == 4)
        assert(result.getOrElse("dolor", 0) == 3)
        assert(result.getOrElse("sit", 0) == 2)
        assert(result.getOrElse("amet", 0) == 1)
      }
    }
  }
}
