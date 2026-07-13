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

import ai.onehouse.lakeloader.configs.ResizerConfig
import scopt.OptionParser

object WorkloadResizerParser {

  val parser: OptionParser[ResizerConfig] =
    new scopt.OptionParser[ResizerConfig]("lake-loader | workload resizer") {
      head("Workload resizer usage")

      opt[String]('i', "input-json")
        .required()
        .action((x, c) => c.copy(inputJson = x))
        .text("Path to a synth-derived.json emitted by WorkloadSynthesizer")

      opt[String]('o', "output-dir")
        .required()
        .action((x, c) => c.copy(outputDir = x))
        .text("Directory where resized-full.flags and resized-summary.flags will be written")

      opt[Double]("scale-factor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("Multiplier applied to per-round record counts. 0.01 = one-hundredth the volume. Default: 1.0 (no scaling)")

      opt[Int]("target-partitions")
        .action((x, c) => c.copy(targetPartitions = Some(x)))
        .text("Override total partition count. If unset, source table's partition count is preserved. " +
          "When smaller than source, the leading N zipf-shape weights are kept; when larger, the fitted " +
          "zipf shape is extrapolated to fill the new partition count. --num-partitions-to-update is " +
          "rescaled to preserve the same fraction of updated partitions as the source workload.")
    }
}
