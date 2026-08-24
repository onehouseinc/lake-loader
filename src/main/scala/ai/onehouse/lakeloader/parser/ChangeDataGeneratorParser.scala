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
import ai.onehouse.lakeloader.configs.{DatagenConfig, KeyTypes, PartitionDistributionSpec, UpdatePatterns}
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

      opt[String]("number-records-per-round")
        .action((x, c) => c.copy(roundsDistribution = x.split(",").map(_.trim.toLong).toList))
        .text(
          "Comma-separated list of record counts per round, or a single value for all rounds. " +
            "If fewer values than rounds, the last value is repeated. Default: 1000000")

      opt[Int]("number-columns")
        .action((x, c) => c.copy(numberColumns = x))
        .text("Number of columns in schema of generated data. Default: 10, minimum 5")

      opt[Int]("record-size")
        .action((x, c) => c.copy(recordSize = x))
        .text("Record Size of the generated data. Default: 1024")

      opt[String]("update-ratio")
        .action((x, c) => c.copy(updateRatios = x.split(",").map(_.trim.toDouble).toList))
        .text("Ratio of updates to total records generated in each incremental batch. Accepts a " +
          "single value applied to all rounds, or a comma-separated list of per-round values. " +
          "If fewer values than rounds are provided, the last value is repeated. Default: 0.5")

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

      opt[String]("update-pattern")
        .action { (x, c) =>
          val parsed = x.split(",").map(_.trim).map(updatePatternsRead.reads).toList
          c.copy(updatePatterns = parsed)
        }
        .text(
          s"Update distribution per round. Accepts a single value applied to all rounds, or a " +
            s"comma-separated list of per-round values. If fewer values than rounds are provided, " +
            s"the last value is repeated. Options: ${UpdatePatterns.values.mkString(", ")}. Default: Uniform")

      opt[KeyType]("primary-key-type")
        .action((x, c) => c.copy(keyType = x))
        .text(s"Primary key type for generated data. Options: ${KeyTypes.values.mkString(", ")}")

      opt[String]("num-partitions-to-update")
        .action((x, c) => c.copy(numPartitionsToUpdate = x.split(",").map(_.trim.toInt).toList))
        .text("Number of partitions that should have at least 1 record written to. Accepts a " +
          "single value applied to all rounds, or a comma-separated list of per-round values. " +
          "If fewer values than rounds are provided, the last value is repeated. Default: -1")

      opt[String]("zipfian-shape")
        .action((x, c) => c.copy(zipfianShapes = x.split(",").map(_.trim.toDouble).toList))
        .text("Shape parameter for zipfian distribution (higher = more skewed). Accepts a single " +
          "value applied to all rounds, or a comma-separated list of per-round values. If fewer " +
          "values than rounds are provided, the last value is repeated. Default: 2.93")

      opt[String]("avro-schema")
        .action((x, c) => c.copy(avroSchemaPath = Some(x)))
        .text(
          "Path to an Avro schema file (.avsc). When provided, data is generated matching this schema " +
            "instead of the default flat schema. The --number-columns parameter is ignored.")

      opt[String]("workload-spec")
        .action((x, c) => c.copy(workloadSpecPath = Some(x)))
        .text("Path to a JSON workload spec file for fine-grained, exact per-partition control. " +
          "Round 0 bootstraps 'totalRecords' evenly across one partition per day in " +
          "[startDate, endDate]; each entry in 'commits' is one round mapping 'yyyy-MM-dd' -> " +
          "{inserts, updates} for only the partitions it touches. When set, --number-rounds, " +
          "--number-records-per-round, --update-ratio, --total-partitions, " +
          "--partition-distribution, --update-pattern and --num-partitions-to-update are ignored.")

      opt[String]("partition-distribution")
        .action((x, c) => c.copy(partitionDistribution = Some(parsePartitionDistribution(x))))
        .text("Per-partition insert weights, given as the leading non-zero entries; the rest are " +
          "zero-padded up to --total-partitions and each segment must sum to 1.0. " +
          "Use ';' to give round 0 a different distribution from subsequent rounds: " +
          "'<first-round>;<subsequent-rounds>'. An empty segment means uniform across all " +
          "partitions for that batch. Examples (with --total-partitions 365): " +
          "'0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1' applies the same skew to every round; " +
          "';0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1' makes round 0 uniform and rounds 1+ skewed.")
    }

  private[lakeloader] def parsePartitionDistribution(raw: String): PartitionDistributionSpec = {
    val parts = raw.split(";", -1)
    require(
      parts.length <= 2,
      s"--partition-distribution accepts at most one ';' separator, got: '$raw'")
    def parseSegment(s: String): Option[List[Double]] = {
      val trimmed = s.trim
      if (trimmed.isEmpty) None
      else Some(trimmed.split(",").map(_.trim.toDouble).toList)
    }
    val first = parseSegment(parts(0))
    val subsequent = if (parts.length == 2) parseSegment(parts(1)) else first
    PartitionDistributionSpec(first, subsequent)
  }
}
