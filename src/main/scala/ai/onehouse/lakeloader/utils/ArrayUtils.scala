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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object ArrayUtils {

  /**
   * Generates a random integer array based on record size and size factor.
   *
   * @param recordSize Desired size of the record in bytes
   * @param sizeFactor Size factor for array length calculation
   * @return Array of sequential integers
   */
  def generateRandomArray(recordSize: Int, sizeFactor: Int): Array[Int] = {
    (0 until recordSize / sizeFactor / 5).toArray
  }

  /**
   * UDF wrapper for generateRandomArray to use in DataFrame transformations.
   */
  val generateRandomArrayUDF: UserDefinedFunction =
    udf((recordSize: Int, sizeFactor: Int) => generateRandomArray(recordSize, sizeFactor))
}
