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

object StringUtils {

  val lineSepBold = "=" * 50
  val lineSepLight = "-" * 50

  private val RANDOM_CHARS: Array[Char] =
    (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "!@#$%^&*()-_=+[]{};:,.<>/?".toSeq).toArray

  def generateRandomString(length: Int): String =
    generateRandomString(length, new scala.util.Random())

  def generateRandomString(length: Int, random: scala.util.Random): String = {
    val arr = new Array[Char](length)
    var i = 0
    while (i < length) {
      arr(i) = RANDOM_CHARS(random.nextInt(RANDOM_CHARS.length))
      i += 1
    }
    new String(arr)
  }

}
