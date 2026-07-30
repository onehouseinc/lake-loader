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
import org.apache.spark.sql.types.{DataType, VariantType}
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.VariantVal

/**
 * Spark 4 implementation of the VARIANT touch points. VARIANT only exists in Spark 4
 * (`VariantType`, `VariantVal`, `parse_json`), so every API usage that would not compile against
 * Spark 3.5 is isolated here — see the `scala-spark3` counterpart.
 */
object VariantUtil {

  val isSupported: Boolean = true

  /** No-op on the Spark 4 build, where VARIANT is available. */
  def requireSupported(): Unit = ()

  /** The VARIANT Spark type, for building schemas without a compile-time Spark 4 dependency. */
  def variantType: DataType = VariantType

  /**
   * Builds the external row value for a VARIANT column from a JSON string. Row-based encoding of
   * VARIANT expects a [[VariantVal]] (`AgnosticEncoders.VariantEncoder`), so returning one here lets
   * the row generator populate VARIANT anywhere in a schema — including nested inside structs,
   * arrays and maps — with no post-processing pass over the DataFrame.
   */
  def makeVariant(json: String): Any = {
    val variant = VariantBuilder.parseJson(json, /* allowDuplicateKeys = */ false)
    new VariantVal(variant.getValue, variant.getMetadata)
  }

  /**
   * Converts the named JSON-string columns of `df` into VARIANT columns, in place. Used to
   * regenerate top-level VARIANT values for update batches, which are read back from parquet as
   * already-typed VARIANT columns rather than built row by row.
   */
  def parseJsonColumns(df: DataFrame, columnNames: Seq[String]): DataFrame =
    columnNames.foldLeft(df)((acc, name) => acc.withColumn(name, expr(s"parse_json(`$name`)")))
}
