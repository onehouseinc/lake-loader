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

import ai.onehouse.lakeloader.configs.WriteProfilerConfig
import scopt.OptionParser

object WriteProfilerParser {

  val parser: OptionParser[WriteProfilerConfig] =
    new scopt.OptionParser[WriteProfilerConfig]("lake-loader | write profiler") {
      head("Write profiler usage")

      opt[String]('t', "table-path")
        .required()
        .action((x, c) => c.copy(tablePath = x))
        .text("Path to an existing Hudi table to profile")

      opt[String]('o', "output-dir")
        .required()
        .action((x, c) => c.copy(outputDir = x))
        .text("Directory where write-profile.txt, write-profile.json and " +
          "file-groups.csv will be written. Never the table path.")

      opt[Int]("max-commits")
        .action((x, c) => c.copy(maxCommits = Some(x)))
        .text("Cap on the number of most-recent completed commits to consider. Default: all")

      opt[String]("since-instant")
        .action((x, c) => c.copy(sinceInstant = Some(x)))
        .text("Only consider commits with instant time >= this value (e.g. 20250101120000)")

      opt[Int]("top-file-groups")
        .action((x, c) => c.copy(topFileGroups = x))
        .text("How many of the heaviest file groups to list individually. Default: 20")

      opt[Boolean]("emit-file-group-csv")
        .action((x, c) => c.copy(emitFileGroupCsv = x))
        .text("Write one CSV row per file group alongside the summary. Default: true")
    }
}
