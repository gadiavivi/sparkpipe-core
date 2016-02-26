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

package software.uncharted.sparkpipe.ops.core.ml.pipeline

import org.scalatest._
import software.uncharted.sparkpipe.Spark
import org.apache.spark.sql.DataFrame
import org.scalatest.mock.MockitoSugar
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReader

class PackageSpec extends FunSpec with MockitoSugar {
  describe("ops.core.ml.pipeline") {

    // val training = sqlContext.createDataFrame(Seq(
    //   (1.0, Vectors.dense(0.0, 1.1, 0.1)),
    //   (0.0, Vectors.dense(2.0, 1.0, -1.0)),
    //   (0.0, Vectors.dense(2.0, 1.3, 1.0)),
    //   (1.0, Vectors.dense(0.0, 1.2, -0.5))
    // )).toDF("label", "features")

    describe("#load()") {
      it("should use Pipeline.load() to load a spark.ml Pipeline") {
        throw new Exception();
      }
    }

    describe("#save()") {
      it("should use pipeline.save() to persist a spark.ml Pipeline") {
        throw new Exception();
      }
    }

    describe("#addStage()") {
      it("should add a PipelineStage to an existing spark.ml Pipeline") {
        throw new Exception();
      }
    }

    describe("#fit()") {
      it("should call fit() on an existing  spark.ml Pipeline, with the given DataFrame") {
        throw new Exception();
      }
    }

    describe("#applyModel()") {
      it("should call transform() on an existing  spark.ml PipelineModel, with the given DataFrame") {
        throw new Exception();
      }
    }
  }
}
