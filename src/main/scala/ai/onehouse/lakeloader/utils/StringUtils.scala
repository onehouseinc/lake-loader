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
import org.apache.spark.sql.types.StructType

object StringUtils {

  val lineSepBold = "=" * 50

  def generateRandomString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "!@#$%^&*()-_=+[]{};:,.<>/?".toSeq
    val r = new scala.util.Random()
    (1 to length).map(_ => chars(r.nextInt(chars.length))).mkString
  }

  /**
   * UDF wrapper for generateRandomString to use in DataFrame transformations.
   */
  val generateRandomStringUDF: UserDefinedFunction =
    udf((length: Int) => generateRandomString(length))

  /**
   * Calculates the size factor for random data generation based on desired record size.
   *
   * @param recordSize Desired size of the record in bytes
   * @param schema     Schema of the record
   * @return Size factor to use for generating random field values
   */
  def calculateSizeFactor(recordSize: Int, schema: StructType): Int = {
    Math.max(recordSize / schema.fields.length, 1)
  }

}
