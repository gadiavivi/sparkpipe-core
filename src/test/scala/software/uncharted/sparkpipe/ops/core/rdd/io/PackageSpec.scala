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

package software.uncharted.sparkpipe.ops.core.rdd.io

import org.apache.hadoop.io.compress.{GzipCodec, BZip2Codec}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar
import software.uncharted.sparkpipe.Pipe

class PackageSpec extends FunSpec with MockitoSugar {

  describe("ops.core.rdd.io") {
    describe("#read()") {
      it("should load a file") {
        val mockContext = mock[SparkContext]
        val path = Math.random().toString

        // test
        read(path)(mockContext)

        // verify
        verify(mockContext).textFile(path)
      }

      it("should work just as well with a SQLContext as with a SparkContext") {
        val mockSparkContext = mock[SparkContext]
        val mockSQLContext = mock[SQLContext]
        val path = Math.random().toString

        // Mock the link between them
        when(mockSQLContext.sparkContext).thenReturn(mockSparkContext)

        // test
        read(path)(mockSQLContext)

        // verify
        verify(mockSparkContext).textFile(path)
      }

      it("should work just as well with a Pipe[SQLContext] as with a Pipe[SparkContext]") {
        val mockSparkContext = mock[SparkContext]
        val mockSQLContext = mock[SQLContext]
        val path = Math.random().toString

        // Mock the link between them
        when(mockSQLContext.sparkContext).thenReturn(mockSparkContext)

        // Simple function to run
        val fcn: SparkContext => SparkContext = sc => sc
        val result = Pipe(mockSQLContext).to(fcn).run()

        // verify
        verify(mockSQLContext).sparkContext
        assert(result === mockSparkContext)
      }

      it("should write a set number of partitions") {
        val mockContext = mock[SparkContext]
        val path = Math.random().toString

        // test
        read(path, options = Map("minPartitions" -> "4"))(mockContext)

        // verify
        verify(mockContext).textFile(path, 4)
        verify(mockContext, never()).textFile(path)
      }

      it("shouldn't support arbitrary formats") {
        val mockContext = mock[SparkContext]
        val path = Math.random().toString

        // test
        intercept[AssertionError] {
          read(path, format = "arbitrary")(mockContext)
        }
      }
    }

    describe("#write()") {
      it("should write a file") {
        val mockRDD = mock[RDD[String]]
        val path = Math.random().toString

        // test
        write(path)(mockRDD)

        // verify
        verify(mockRDD).saveAsTextFile(path)
      }

      it("should write a file with BZip compression") {
        val mockRDD = mock[RDD[String]]
        val path = Math.random().toString

        // test
        write(path, options = Map("codec" -> "\t\nBzIp2  "))(mockRDD)

        // verify
        verify(mockRDD).saveAsTextFile(path, classOf[BZip2Codec])
      }

      it("should write a file with GZip compression") {
        val mockRDD = mock[RDD[String]]
        val path = Math.random().toString

        // test
        write(path, options = Map("codec" -> "gzip"))(mockRDD)

        // verify
        verify(mockRDD).saveAsTextFile(path, classOf[GzipCodec])
      }

      it("shouldn't support arbitrary formats") {
        val mockRDD = mock[RDD[String]]
        val path = Math.random().toString

        // test
        intercept[AssertionError] {
          write(path, format = "arbitrary")(mockRDD)
        }
      }
    }
  }
}
