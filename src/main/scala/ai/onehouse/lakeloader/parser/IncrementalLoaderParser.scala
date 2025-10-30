/*
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

package ai.onehouse.lakeloader.parser

import ai.onehouse.lakeloader.configs.{LoadConfig, OperationType, StorageFormat}

object IncrementalLoaderParser {

  val parser = new scopt.OptionParser[LoadConfig]("lake-loader | incremental loader") {
    head("lake-loader", "1.x")

    opt[Int]("number-rounds")
      .action((x, c) => c.copy(numberOfRounds = x))
      .text("Number of rounds of incremental change data to generate. Default 10.")

    opt[String]('i', "input-path")
      .required()
      .action((x, c) => c.copy(inputPath = x))
      .text("Input path")

    opt[String]('o', "output-path")
      .required()
      .action((x, c) => c.copy(outputPath = x))
      .text("Output path")

    opt[Int]("parallelism")
      .required()
      .action((x, c) => c.copy(parallelism = x))
      .text("Parallelism")

    opt[String]("format")
      .required()
      .action((x, c) => c.copy(format = x))
      .validate { x =>
        if (StorageFormat.values().contains(x))
          Right(())
        else
          Left(s"Invalid format: '$x'. Allowed: ${StorageFormat.values().mkString(", ")}")
      }
      .text("Format to load data into. Options: " + StorageFormat.values().mkString(", "))

    opt[String]("operation-type")
      .action((x, c) => c.copy(operationType = x))
      .validate { x =>
        if (OperationType.values().contains(x))
          Right(())
        else
          Left(s"Invalid operation: '$x'. Allowed: ${OperationType.values().mkString(", ")}")
      }
      .text("Write operation type")

    opt[Map[String, String]]("options")
      .action((x, c) => c.copy(options = x))
      .text("Options")

    opt[Boolean]("non-partitioned")
      .action((x, c) => c.copy(nonPartitioned = x))
      .text("Non partitioned")

    opt[String]('e', "experiment-id")
      .action((x, c) => c.copy(experimentId = x))
      .text("Experiment ID")

    opt[Int]("start-round")
      .action((x, c) => c.copy(startRound = x))
      .text("Start round for incremental loading. Default 0.")

    opt[String]("catalog")
      .action((x, c) => c.copy(catalog = x))
      .text("Catalog name. Default spark_catalog.")

    opt[String]("database")
      .action((x, c) => c.copy(database = x))
      .text("Database name. Default default.")

    opt[Seq[String]]("additional-merge-condition-columns")
      .action((x, c) => c.copy(additionalMergeConditionColumns = x))
      .text("Additional columns to append to merge condition on top of defaults. Default base: [key] for non-partitioned, [key, partition] for partitioned.")
  }

  /**
   * Get columns to be used for merge condition (ON clause).
   */
  def getMergeConditionColumns(config: LoadConfig): Seq[String] = {
    val baseColumns = if (config.nonPartitioned) {
      Seq("key")
    } else {
      Seq("key", "partition")
    }
    baseColumns ++ config.additionalMergeConditionColumns
  }
}
