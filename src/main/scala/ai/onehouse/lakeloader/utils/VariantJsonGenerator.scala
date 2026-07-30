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

package ai.onehouse.lakeloader.utils

import java.io.Serializable

import scala.util.Random

/**
 * Generates the JSON payloads backing VARIANT columns.
 *
 * The generated object has a stable shape across records — same keys, same value types at the same
 * paths — while the leaf values are randomized. A stable shape is what makes the payloads
 * representative of real semi-structured data (and what lets engines shred them); fully random keys
 * would instead measure metadata blowup.
 *
 * At every level the object carries `numKeys` entries whose value types rotate through
 * string / long / double / boolean / array, and when there is nesting budget left, the last entry is
 * a nested object one level deeper.
 */
object VariantJsonGenerator extends Serializable {

  /** Column name prefix used for generated VARIANT columns. */
  val VARIANT_FIELD_PREFIX: String = "variantField"

  /** Spark's `DataType.typeName` for VARIANT; usable without a compile-time Spark 4 dependency. */
  val VARIANT_TYPE_NAME: String = "variant"

  private val ARRAY_LENGTH = 3

  /**
   * @param numKeys      number of keys per JSON object level
   * @param nestingDepth object nesting depth; 1 means a flat object
   * @param sizeFactor   approximate byte budget for the payload, spent on string leaves
   * @param random       source of randomness for leaf values
   */
  def generateJson(numKeys: Int, nestingDepth: Int, sizeFactor: Int, random: Random): String = {
    require(numKeys >= 1, s"numKeys must be at least 1, got $numKeys")
    require(nestingDepth >= 1, s"nestingDepth must be at least 1, got $nestingDepth")
    generateObject(numKeys, nestingDepth, sizeFactor, random)
  }

  private def generateObject(
      numKeys: Int,
      remainingDepth: Int,
      sizeFactor: Int,
      random: Random): String = {
    // Split the byte budget across this level's keys; nested levels get their share recursively.
    val perKeySize = Math.max(sizeFactor / numKeys, 1)
    val entries = (0 until numKeys).map { i =>
      val isLastKey = i == numKeys - 1
      val value =
        if (isLastKey && remainingDepth > 1) {
          generateObject(numKeys, remainingDepth - 1, perKeySize, random)
        } else {
          generateLeaf(i, perKeySize, random)
        }
      s""""k$i":$value"""
    }
    entries.mkString("{", ",", "}")
  }

  private def generateLeaf(keyIndex: Int, sizeFactor: Int, random: Random): String =
    keyIndex % 5 match {
      case 0 => quote(StringUtils.generateRandomString(Math.max(sizeFactor, 1)))
      case 1 => random.nextLong().toString
      case 2 => random.nextDouble().toString
      case 3 => random.nextBoolean().toString
      case 4 =>
        val elementSize = Math.max(sizeFactor / ARRAY_LENGTH, 1)
        (0 until ARRAY_LENGTH)
          .map(_ => quote(StringUtils.generateRandomString(elementSize)))
          .mkString("[", ",", "]")
    }

  private def quote(s: String): String = s""""$s""""

  /**
   * Names of the VARIANT columns appended after the `numColumns` regular columns.
   */
  def variantColumnNames(numColumns: Int, numVariantColumns: Int): Seq[String] =
    (0 until numVariantColumns).map(i => s"$VARIANT_FIELD_PREFIX${numColumns + i}")
}
