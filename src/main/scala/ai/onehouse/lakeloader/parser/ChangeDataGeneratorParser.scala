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

import ai.onehouse.lakeloader.configs.KeyTypes.KeyType
import ai.onehouse.lakeloader.configs.UpdatePatterns.UpdatePatterns
import ai.onehouse.lakeloader.configs.{DatagenConfig, KeyTypes, UpdatePatterns}
import ai.onehouse.lakeloader.configs.ChangeDataGeneratorConfigs._
import scopt.OptionParser

object ChangeDataGeneratorParser {

  val parser: OptionParser[DatagenConfig] =
    new scopt.OptionParser[DatagenConfig]("lake-loader | change data generator") {
      head("Change data generator usage")

      opt[String]('p', "path")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("Output path")

      opt[Int]("number-rounds")
        .action((x, c) => c.copy(numberOfRounds = x))
        .text("Number of rounds of incremental change data to generate. Default: 10")

      opt[Long]("number-records-per-round")
        .action((x, c) => c.copy(numberRecordsPerRound = x))
        .text("Number of columns in schema of generated data. Default: 1000000")

      opt[Int]("number-columns")
        .action((x, c) => c.copy(numberColumns = x))
        .text("Number of columns in schema of generated data. Default: 10, minimum 5")

      opt[Int]("record-size")
        .action((x, c) => c.copy(recordSize = x))
        .text("Record Size of the generated data. Default: 1024")

      opt[Double]("update-ratio")
        .action((x, c) => c.copy(updateRatio = x))
        .text("Ratio of updates to total records generated in each incremental batch. Default: 0.5")

      opt[Int]("total-partitions")
        .action((x, c) => c.copy(totalPartitions = x))
        .text("Total number of partitions desired for the benchmark table. Default: unpartitioned.")

      opt[Int]("datagen-file-size")
        .action((x, c) => c.copy(targetDataFileSize = x))
        .text("Target data file size for the data generated files. Default: 128MB")

      opt[Boolean]("skip-if-exists")
        .action((x, c) => c.copy(skipIfExists = x))
        .text("Skip generated data if folder already exists. Default: false")

      opt[Int]("start-round")
        .action((x, c) => c.copy(startRound = x))
        .text("Generate data from specified round. Default: 0")

      opt[UpdatePatterns]("update-pattern")
        .action((x, c) => c.copy(updatePattern = x))
        .text(
          s"The pattern for the updates to be generated for the data. Options: ${UpdatePatterns.values.mkString(", ")}. Default: ")

      opt[KeyType]("primary-key-type")
        .action((x, c) => c.copy(keyType = x))
        .text(s"Primary key type for generated data. Options: ${KeyTypes.values.mkString(", ")}")

      opt[Int]("num-partitions-to-update")
        .action((x, c) => c.copy(numPartitionsToUpdate = x))
        .text("Number of partitions that should have at least 1 records written to.")

      opt[Double]("zipfian-shape")
        .action((x, c) => c.copy(zipfianShape = x))
        .text("Shape parameter for zipfian distribution (higher = more skewed). Default: 2.93")
    }
}
