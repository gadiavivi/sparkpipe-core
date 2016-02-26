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

package software.uncharted.sparkpipe.ops.core.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline => MLPipeline, PipelineStage => MLPipelineStage, PipelineModel => MLPipelineModel}

/**
 * Lightweight helpers for using spark.ml Pipelines with sparkpipe.
 * The goal is not to replace the spark.ml pipeline, but rather to
 * smooth its integration with other logic and libraries supporting
 * the sparkpipe ops format.
 */
package object pipeline {

  /**
   * Load a spark.ml Pipeline from a file
   * @param path the path to the persisted Pipeline
   * @return a spark.ml Pipeline constructed from the given file
   */
  // $COVERAGE-OFF$
  def load(path: String): MLPipeline = {
    MLPipeline.load(path)
  }
  // $COVERAGE-ON$

  /**
   * Persist a spark.ml Pipeline to a file
   * @param path the path for the persisted Pipeline file
   * @return the input spark.ml Pipeline, unchanged
   */
  def save(path: String)(mlpipe: MLPipeline): MLPipeline = {
    mlpipe.save(path)
    mlpipe
  }

  /**
   * Add a stage (an Estimator or a Transformer) to an existing spark.ml Pipeline
   * @param stage a spark.ml PipelineStage (an Estimator or Transformer)
   * @return the input Pipeline, with the added stage
   */
  def addStage(stage: MLPipelineStage)(mlpipe: MLPipeline): MLPipeline = {
    mlpipe.setStages(mlpipe.getStages ++ Array(stage))
    mlpipe
  }

  /**
   * Fit a spark.ml pipeline to a DataFrame to produce a PipelineModel.
   * This op is intended to be used after merging two Pipes, one providng
   * the spark.ml Pipeline, and the other providing the DataFrame.
   * For example, Pipe(mlPipelinePipe, dataPipe).to(ops.core.ml.fit)
   * @param args a Tuple2 including a spark.ml Pipeline and a DataFrame
   * @return the fitted spark.ml PipelineModel
   */
  def fit(args: (MLPipeline, DataFrame)): MLPipelineModel = {
    args._1.fit(args._2)
  }

  /**
   * Apply a spark.ml PipelineModel to a DataFrame to make a prediction.
   * This op is intended to be used after merging two Pipes, one providng
   * the spark.ml PipelineModel, and the other providing the DataFrame.
   * For example, Pipe(mlModelPipe, dataPipe).to(ops.core.ml.applyModel)
   * @param args a Tuple2 including a spark.ml PipelineModel and a DataFrame
   * @return the resultant DataFrame
   */
  def applyModel(args: (MLPipelineModel, DataFrame)): DataFrame = {
    args._1.transform(args._2)
  }
}
