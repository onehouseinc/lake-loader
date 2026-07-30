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

package ai.onehouse.lakeloader.configs

import ai.onehouse.lakeloader.configs.KeyTypes.KeyType
import ai.onehouse.lakeloader.configs.UpdatePatterns.UpdatePatterns

case class DatagenConfig(
    outputPath: String = "",
    numberOfRounds: Int = 10,
    roundsDistribution: List[Long] = List(1000000L),
    numberColumns: Int = 10,
    recordSize: Int = 1024,
    updateRatio: Double = 0.5f,
    totalPartitions: Int = -1,
    targetDataFileSize: Int = 128 * 1024 * 1024,
    skipIfExists: Boolean = false,
    startRound: Int = 0,
    keyType: KeyType = KeyTypes.Random,
    updatePattern: UpdatePatterns = UpdatePatterns.Uniform,
    numPartitionsToUpdate: Int = -1,
    zipfianShape: Double = 2.93,
    avroSchemaPath: Option[String] = None,
    sparkSchemaPath: Option[String] = None,
    partitionDistribution: Option[PartitionDistributionSpec] = None,
    numVariantColumns: Int = 0,
    variantNumKeys: Int = 8,
    variantNestingDepth: Int = 1)

/**
 * Shape of the generated VARIANT columns.
 *
 * @param numColumns    number of VARIANT columns appended after the regular columns
 * @param numKeys       number of keys in each JSON object level
 * @param nestingDepth  JSON object nesting depth; 1 means a flat object
 */
case class VariantSpec(numColumns: Int = 0, numKeys: Int = 8, nestingDepth: Int = 1) {
  require(numColumns >= 0, s"Number of variant columns cannot be negative, got $numColumns")
  require(numColumns == 0 || numKeys >= 1, s"Variant columns need at least 1 key, got $numKeys")
  require(
    numColumns == 0 || nestingDepth >= 1,
    s"Variant nesting depth must be at least 1, got $nestingDepth")

  def isEnabled: Boolean = numColumns > 0
}

object VariantSpec {
  val disabled: VariantSpec = VariantSpec()
}

/**
 * Per-round split for the CLI partition distribution flag.
 *
 * `firstRound` weights apply to round 0; `subsequentRounds` weights apply to rounds 1..N-1.
 * `None` for a segment means "uniform across totalPartitions" for that batch.
 * Each segment carries only the leading non-zero weights and is zero-padded to totalPartitions
 * when the matrix is built.
 */
case class PartitionDistributionSpec(
    firstRound: Option[List[Double]],
    subsequentRounds: Option[List[Double]])

object KeyTypes extends Enumeration {
  type KeyType = Value
  val Random, TemporallyOrdered = Value
}

object UpdatePatterns extends Enumeration {
  type UpdatePatterns = Value
  val Uniform, Zipf = Value
}

object ChangeDataGeneratorConfigs {
  implicit val keyTypeRead: scopt.Read[KeyType] = scopt.Read.reads { s =>
    try {
      KeyTypes.withName(s)
    } catch {
      case _: NoSuchElementException =>
        throw new IllegalArgumentException(
          s"Invalid key type: $s. Valid values: ${KeyTypes.values.mkString(", ")}")
    }
  }

  implicit val updatePatternsRead: scopt.Read[UpdatePatterns] = scopt.Read.reads { s =>
    try {
      UpdatePatterns.withName(s)
    } catch {
      case _: NoSuchElementException =>
        throw new IllegalArgumentException(
          s"Invalid update pattern: $s. Valid values: ${UpdatePatterns.values.mkString(", ")}")
    }
  }
}
