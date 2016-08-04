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
import org.apache.spark.SparkContext
import software.uncharted.sparkpipe.Spark
import org.apache.spark.sql.DataFrame
import org.scalatest.mock.MockitoSugar
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReader
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.{Pipeline => MLPipeline, PipelineStage => MLPipelineStage, PipelineModel => MLPipelineModel}


class PackageSpec extends FunSpec with MockitoSugar {
  describe("ops.core.ml.pipeline") {

    val training = Spark.sparkSession.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    val test = Spark.sparkSession.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
    val path = "/tmp/unfit-lr-model";
    val sc = Spark.sc

    // TODO figure out how to deal with static load method
    // describe("#load()") {
    //   it("should use Pipeline.load() to load a spark.ml Pipeline") {
    //     throw new Exception // Test not implemented
    //   }

    describe("#save()") {
      it("should use pipeline.save() to persist a spark.ml Pipeline") {
        if (Spark.sc.version >= "1.6.0") {
          val mockPipeline = mock[MLPipeline]
          save(sc, path)(mockPipeline)
          verify(mockPipeline).save(path)
        } else {
          intercept[UnsupportedOperationException] {
            val mockPipeline = mock[MLPipeline]
            save(sc, path)(mockPipeline)
          }
        }
      }
    }


    describe("#addStage()") {
      it("should add a PipelineStage to an existing spark.ml Pipeline") {
        val pipeline = new Pipeline()
        pipeline.setStages(Array(tokenizer, hashingTF))

        // Test
        addStage(lr)(pipeline)

        // Verify
        equals(pipeline.getStages, Array(tokenizer, hashingTF, lr))
      }
    }

    describe("#fit()") {
      it("should call fit() on an existing  spark.ml Pipeline, with the given DataFrame") {
        val mockPipeline = mock[MLPipeline]

        // Test
        fit((mockPipeline, training))

        // Verify
        verify(mockPipeline).fit(training)
      }
    }

    describe("#applyModel()") {
      it("should call transform() on an existing  spark.ml PipelineModel, with the given DataFrame") {
        val mockPipelineModel = mock[MLPipelineModel]

        // Test
        applyModel((mockPipelineModel, test))

        // Verify
        verify(mockPipelineModel).transform(test)
      }
    }
  }
}
