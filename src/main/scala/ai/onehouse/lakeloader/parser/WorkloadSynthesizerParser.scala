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

import ai.onehouse.lakeloader.configs.ChangeDataGeneratorConfigs._
import ai.onehouse.lakeloader.configs.SynthesizerConfig
import scopt.OptionParser

object WorkloadSynthesizerParser {

  val parser: OptionParser[SynthesizerConfig] =
    new scopt.OptionParser[SynthesizerConfig]("lake-loader | workload synthesizer") {
      head("Workload synthesizer usage")

      opt[String]('t', "table-path")
        .required()
        .action((x, c) => c.copy(tablePath = x))
        .text("Path to an existing Hudi table to characterize")

      opt[String]('o', "output-dir")
        .required()
        .action((x, c) => c.copy(outputDir = x))
        .text("Directory where synth-full.flags, synth-summary.flags, and synth-audit.txt will be written")

      opt[Int]("max-commits")
        .action((x, c) => c.copy(maxCommits = Some(x)))
        .text("Cap on the number of most-recent completed commits to consider. Default: all completed commits")

      opt[String]("since-instant")
        .action((x, c) => c.copy(sinceInstant = Some(x)))
        .text("Only consider commits with instant time >= this value (Hudi instant string, e.g. 20250101120000)")

      opt[Double]("min-zipf-shape")
        .action((x, c) => c.copy(minZipfShapeToEmit = x))
        .text("Minimum fitted zipf shape below which we emit Uniform instead of Zipf. Default: 0.3")

      opt[Int]("key-sample-commits")
        .action((x, c) => c.copy(keySampleCommits = x))
        .text("Number of most-recent completed commits whose written base parquet files are used as " +
          "the sample source for key-type inference. Small (default 3) because recent commits reflect " +
          "steady-state workload and give deterministic sample selection (unlike full-table directory walks).")

      opt[Int]("key-sample-files")
        .action((x, c) => c.copy(keySampleFiles = x))
        .text("Cap on the total number of base parquet files sampled across --key-sample-commits. " +
          "Default: 100")

      opt[Int]("key-sample-size")
        .action((x, c) => c.copy(keySampleSize = x))
        .text("Fallback: if fewer than 3 base files are available for footer sampling, read up to this " +
          "many actual record-key values from one file for classification. Default: 500")

      opt[String]("primary-key-type")
        .action((x, c) => c.copy(primaryKeyTypeOverride = Some(keyTypeRead.reads(x))))
        .text("Skip primary-key inference and use this value instead (Random | TemporallyOrdered)")

      opt[String]("schema-file")
        .action((x, c) => c.copy(schemaFile = Some(x)))
        .text("Path to a customer-supplied Avro schema (.avsc). If set, the emitted flag files " +
          "reference this schema via --avro-schema and drop --number-columns. If not set, the tool " +
          "reads the source Hudi table's schema and emits --number-columns matching its top-level " +
          "field count so the generator can produce data with the same column arity.")

      opt[Boolean]("anonymize-schema")
        .action((x, c) => c.copy(anonymizeSchema = x))
        .text("If true, rewrite field names in the emitted schema.avsc to typed placeholders " +
          "(col_int_a, col_long_b, col_string_c, ...). Data types and nullability are preserved; " +
          "original column names never leave the customer environment. Applies to both " +
          "customer-supplied schemas and schemas inferred from the source Hudi table. Default: false.")
    }
}
