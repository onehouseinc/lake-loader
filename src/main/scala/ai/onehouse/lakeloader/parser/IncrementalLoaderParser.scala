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

import ai.onehouse.lakeloader.configs.{ApiType, LoadConfig, MergeMode, OperationType, StorageFormat, WriteMode}
import ai.onehouse.lakeloader.ChangeDataGenerator.{PARTITION_PATH_FIELD_NAME, RECORD_KEY_FIELD_NAME}

object IncrementalLoaderParser {

  val parser = new scopt.OptionParser[LoadConfig]("lake-loader | incremental loader") {
    head("lake-loader", "1.x")

    opt[Int]("number-rounds")
      .action((x, c) => c.copy(numberOfRounds = x))
      .text("Number of rounds of incremental change data to generate. Default: 10")

    opt[String]('i', "input-path")
      .required()
      .action((x, c) => c.copy(inputPath = x))
      .text("Input path")

    opt[String]('o', "output-path")
      .required()
      .action((x, c) => c.copy(outputPath = x))
      .text("Output path")

    opt[StorageFormat]("format")
      .action((x, c) => c.copy(format = x.asString))
      .text(
        s"Format to load data into. Options: ${StorageFormat.values().mkString(", ")}. Default: hudi")

    opt[OperationType]("initial-operation-type")
      .action((x, c) => c.copy(initialOperationType = x.asString))
      .text(
        s"Write operation type for the first batch. Options: ${OperationType.values().mkString(", ")}. Default: bulk_insert")

    opt[OperationType]("operation-type")
      .action((x, c) => c.copy(operationType = x.asString))
      .text(
        s"Write operation type for subsequent batches. Options: ${OperationType.values().mkString(", ")}. Default: upsert")

    opt[ApiType]("api-type")
      .action((x, c) => c.copy(apiType = x.asString))
      .text(
        s"Api type, to be used with Hudi format only. Options: ${ApiType.values().mkString(", ")}. Default: spark-datasource")

    opt[Map[String, String]]("initial-options")
      .action((x, c) => c.copy(initialOptions = x))
      .text("Options for first batch. Default: empty map")

    opt[Map[String, String]]("options")
      .action((x, c) => c.copy(options = x))
      .text("Options for second and subsequent batches. Default: empty map")

    opt[Boolean]("non-partitioned")
      .action((x, c) => c.copy(nonPartitioned = x))
      .text("Non partitioned. Default: false")

    opt[String]('e', "experiment-id")
      .action((x, c) => c.copy(experimentId = x))
      .text("Experiment ID. Default: random string of length 10")

    opt[Int]("start-round")
      .action((x, c) => c.copy(startRound = x))
      .text("Start round for incremental loading. Default: 0")

    opt[String]("catalog")
      .action((x, c) => c.copy(catalog = x))
      .text("Catalog name. Default: spark_catalog")

    opt[String]("database")
      .action((x, c) => c.copy(database = x))
      .text("Database name. Default: default")

    opt[Seq[String]]("additional-merge-condition-columns")
      .action((x, c) => c.copy(additionalMergeConditionColumns = x))
      .text("Additional columns to append to merge condition on top of defaults. Default base: [key] for non-partitioned, [key, partition] for partitioned")

    opt[MergeMode]("merge-mode")
      .action((x, c) => c.copy(mergeMode = x.asString))
      .text(
        s"Merge mode for upsert operations. Options: ${MergeMode.values().mkString(", ")}. Default: update-insert")

    opt[Seq[String]]("update-columns")
      .action((x, c) => c.copy(updateColumns = x))
      .text("Columns to update during merge operations. If not specified, all columns will be updated. Default: all columns")

    opt[WriteMode]("write-mode")
      .action((x, c) => c.copy(writeMode = x.asString))
      .text(
        s"Write mode for updates/deletes. Applies to Hudi (table type), Delta (deletion vectors), and Iceberg. Options: ${WriteMode.values().mkString(", ")}. Default: copy-on-write")

    opt[Boolean]("async-compaction")
      .action((x, c) => c.copy(asyncCompactionEnabled = x))
      .text("Enable async background compaction. Default: false")

    opt[Int]("compaction-frequency-commits")
      .action((x, c) => c.copy(compactionFrequencyCommits = x))
      .text("Schedule compaction every N commits. Default: 3")

    opt[Boolean]("run-final-compaction")
      .action((x, c) => c.copy(runFinalCompaction = x))
      .text("Run final compaction on shutdown. Default: true")
  }

  /**
   * Get columns to be used for merge condition (ON clause).
   */
  def getMergeConditionColumns(config: LoadConfig): Seq[String] = {
    val baseColumns = if (config.nonPartitioned) {
      Seq(RECORD_KEY_FIELD_NAME)
    } else {
      Seq(RECORD_KEY_FIELD_NAME, PARTITION_PATH_FIELD_NAME)
    }
    baseColumns ++ config.additionalMergeConditionColumns
  }
}
