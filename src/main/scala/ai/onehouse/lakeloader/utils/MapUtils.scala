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
import java.util.UUID.randomUUID
import scala.util.Random

object MapUtils {

  /**
   * Generates a random map based on record size and size factor.
   *
   * @param recordSize Desired size of the record in bytes
   * @param sizeFactor Size factor for map size calculation
   * @return Map with UUID keys and random integer values
   */
  def generateRandomMap(recordSize: Int, sizeFactor: Int): Map[String, Int] = {
    (0 until recordSize / sizeFactor / 2 / 40)
      .map(_ => (randomUUID().toString, Random.nextInt()))
      .toMap
  }

  /**
   * UDF wrapper for generateRandomMap to use in DataFrame transformations.
   */
  val generateRandomMapUDF: UserDefinedFunction =
    udf((recordSize: Int, sizeFactor: Int) => generateRandomMap(recordSize, sizeFactor))
}
