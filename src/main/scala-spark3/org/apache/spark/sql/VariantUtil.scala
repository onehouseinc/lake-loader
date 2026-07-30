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

package org.apache.spark.sql

/**
 * Spark 3.5 stub for the VARIANT touch points. Spark 3.5 has no `VariantType` and no `parse_json`,
 * and `iceberg-spark-runtime-3.5` ships no variant readers/writers, so VARIANT is unavailable on
 * this build — see the `scala-spark4` counterpart for the real implementation.
 */
object VariantUtil {

  private val UNSUPPORTED_MESSAGE =
    ("The VARIANT type requires the Spark 4 build of lake-loader. This jar was built with -Pspark3 "
      + "(Spark 3.5 has no VariantType, and iceberg-spark-runtime-3.5 has no variant support). "
      + "Rebuild without -Pspark3 to generate or load VARIANT columns.")

  val isSupported: Boolean = false

  /** Always fails on the Spark 3.5 build, where VARIANT is unavailable. */
  def requireSupported(): Unit = throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE)

  def parseJsonColumns(df: DataFrame, columnNames: Seq[String]): DataFrame = {
    if (columnNames.nonEmpty) requireSupported()
    df
  }
}
