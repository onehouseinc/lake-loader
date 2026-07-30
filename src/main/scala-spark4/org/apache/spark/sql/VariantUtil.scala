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

import org.apache.spark.sql.functions.expr

/**
 * Spark 4 implementation of the VARIANT touch points. VARIANT only exists in Spark 4
 * (`org.apache.spark.sql.types.VariantType`, SQL function `parse_json`), so every API usage that
 * would not compile against Spark 3.5 is isolated here — see the `scala-spark3` counterpart.
 */
object VariantUtil {

  val isSupported: Boolean = true

  /** No-op on the Spark 4 build, where VARIANT is available. */
  def requireSupported(): Unit = ()

  /**
   * Converts the named JSON-string columns of `df` into VARIANT columns, in place.
   */
  def parseJsonColumns(df: DataFrame, columnNames: Seq[String]): DataFrame =
    columnNames.foldLeft(df)((acc, name) => acc.withColumn(name, expr(s"parse_json(`$name`)")))
}
