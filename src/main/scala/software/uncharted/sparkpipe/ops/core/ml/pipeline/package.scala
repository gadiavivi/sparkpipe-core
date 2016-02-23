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

  def load(path: String): MLPipeline = {
    MLPipeline.load(path)
  }

  def save(path: String)(mlpipe: MLPipeline): MLPipeline = {
    mlpipe.save(path)
    mlpipe
  }

  def addStage(stage: MLPipelineStage)(mlpipe: MLPipeline): MLPipeline = {
    mlpipe.setStages(mlpipe.getStages ++ Array(stage))
    mlpipe
  }

  def fit(args: (MLPipeline, DataFrame)): MLPipelineModel = {
    args._1.fit(args._2)
  }

  def applyModel(args: (MLPipelineModel, DataFrame)): DataFrame = {
    args._1.transform(args._2)
  }
}
