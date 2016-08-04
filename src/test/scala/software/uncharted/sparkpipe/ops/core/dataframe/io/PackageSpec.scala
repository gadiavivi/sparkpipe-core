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

package software.uncharted.sparkpipe.ops.core.dataframe.io

import org.scalatest._
import software.uncharted.sparkpipe.Spark
import org.apache.spark.sql.{SparkSession, DataFrame, DataFrameReader, DataFrameWriter}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._

class PackageSpec extends FunSpec with MockitoSugar {

  describe("ops.core.dataframe.io") {
    describe("#read()") {
      it("should pass arguments to the underlying SparkSession.read() API") {
        val mockSparkSession = mock[SparkSession]
        val mockReader = mock[DataFrameReader]
        val path = Math.random().toString
        val format = Math.random().toString
        val options = Map(Math.random().toString -> Math.random().toString)
        val schema = StructType(Seq(
          StructField("year", IntegerType, true),
          StructField("month", IntegerType, true)
        ))

        // mock methods
        when(mockSparkSession.read).thenReturn(mockReader)
        when(mockReader.format(anyString())).thenReturn(mockReader)
        when(mockReader.options(options)).thenReturn(mockReader)
        when(mockReader.schema(schema)).thenReturn(mockReader)

        // test
        read(path, format, options, schema)(mockSparkSession)

        // verify
        verify(mockReader).format(format)
        verify(mockReader).options(options)
        verify(mockReader).schema(schema)
        verify(mockReader).load(path)
      }

      it("should call load() instead of load(String) when an empty path is specified") {
        val mockSparkSession = mock[SparkSession]
        val mockReader = mock[DataFrameReader]
        val path = ""
        val format = Math.random().toString
        val options = Map(Math.random().toString -> Math.random().toString)
        val schema = StructType(Seq(
          StructField("year", IntegerType, true),
          StructField("month", IntegerType, true)
        ))

        // mock methods
        when(mockSparkSession.read).thenReturn(mockReader)
        when(mockReader.format(anyString())).thenReturn(mockReader)
        when(mockReader.options(options)).thenReturn(mockReader)
        when(mockReader.schema(schema)).thenReturn(mockReader)

        // test
        read(path, format, options, schema)(mockSparkSession)

        // verify
        verify(mockReader).format(format)
        verify(mockReader).options(options)
        verify(mockReader).schema(schema)
        verify(mockReader).load()
      }

      it("should not call schema() when an empty schema is specified") {
        val mockSparkSession = mock[SparkSession]
        val mockReader = mock[DataFrameReader]
        val path = ""
        val format = Math.random().toString
        val options = Map(Math.random().toString -> Math.random().toString)
        val schema = new StructType(Array[StructField]())

        // mock methods
        when(mockSparkSession.read).thenReturn(mockReader)
        when(mockReader.format(anyString())).thenReturn(mockReader)
        when(mockReader.options(options)).thenReturn(mockReader)

        // test
        read(path, format, options, schema)(mockSparkSession)

        // verify
        verify(mockReader).format(format)
        verify(mockReader).options(options)
        verify(mockReader, never()).schema(schema)
        verify(mockReader).load()
      }
    }

    // TODO uncomment when DataFrameWriter isn't final
    // describe("#write()") {
    //   it("should pass arguments to the underlying DataFrame.write() API") {
    //     val mockDf = mock[DataFrame]
    //     val mockWriter = mock[DataFrameWriter]
    //     val path = Math.random().toString
    //     val format = Math.random().toString
    //     val options = Map(Math.random().toString -> Math.random().toString)
    //
    //     // mock methods
    //     when(mockDf.write).thenReturn(mockWriter)
    //     when(mockWriter.format(anyString())).thenReturn(mockWriter)
    //     when(mockWriter.options(options)).thenReturn(mockWriter)
    //
    //     // test
    //     write(path, format, options)(mockDf)
    //
    //     // verify
    //     verify(mockWriter).format(format)
    //     verify(mockWriter).options(options)
    //     verify(mockWriter).save(path)
    //   }
    //
    //   it("should call save() instead of save(String) when an empty path is specified") {
    //     val mockDf = mock[DataFrame]
    //     val mockWriter = mock[DataFrameWriter]
    //     val path = ""
    //     val format = Math.random().toString
    //     val options = Map(Math.random().toString -> Math.random().toString)
    //
    //     // mock methods
    //     when(mockDf.write).thenReturn(mockWriter)
    //     when(mockWriter.format(anyString())).thenReturn(mockWriter)
    //     when(mockWriter.options(options)).thenReturn(mockWriter)
    //
    //     // test
    //     write(path, format, options)(mockDf)
    //
    //     // verify
    //     verify(mockWriter).format(format)
    //     verify(mockWriter).options(options)
    //     verify(mockWriter).save()
    //   }
    // }
  }
}
