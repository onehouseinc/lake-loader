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
    numberRecordsPerRound: Long = 1000000,
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
    zipfianShape: Double = 2.93)

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
