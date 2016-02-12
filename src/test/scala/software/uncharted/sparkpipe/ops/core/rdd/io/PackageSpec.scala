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

package software.uncharted.sparkpipe.ops.core.rdd.io

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar

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
    }
  }
}
