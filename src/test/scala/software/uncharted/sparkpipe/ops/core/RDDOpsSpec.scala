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

package software.uncharted.salt.core.analytic.numeric

import org.scalatest._
import software.uncharted.sparkpipe.ops.core.RDDOps
import org.apache.spark.storage.StorageLevel
import software.uncharted.sparkpipe.Spark

class RDDOpsSpec extends FunSpec {
  describe("RDDOps") {
    describe("#toDF()") {
      it("should convert an input RDD with a compatible record type into a DataFrame") {
        val rdd = Spark.sc.parallelize(Seq(
          ("one", 1),
          ("two", 2),
          ("three", 3),
          ("four", 4)
        ))
        val df = RDDOps.toDF(Spark.sqlContext)(rdd)
        assert(df.schema(0).dataType.equals(org.apache.spark.sql.types.StringType))
        assert(df.schema(1).dataType.equals(org.apache.spark.sql.types.IntegerType))
        assert(df.first.getString(0).equals("one"))
        assert(df.first.getInt(1).equals(1))
      }
    }
    describe("#cache()") {
      it("should call .cache() on an input RDD") {
        val rdd = Spark.sc.parallelize(Seq(1,2,3,4))
        assert(RDDOps.cache(rdd).getStorageLevel == StorageLevel.MEMORY_ONLY)
      }
    }
  }
}
