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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors

/**
 * Loads a generator schema expressed directly as a Spark schema.
 *
 * Unlike Avro, a Spark schema can express every type the generator supports, including VARIANT,
 * and VARIANT may appear anywhere — top level or nested inside a struct, array or map. Two
 * interchangeable encodings are accepted:
 *
 *  - DDL, e.g. `key STRING, payload VARIANT, nested STRUCT<a: VARIANT, b: INT>`
 *  - Spark schema JSON, i.e. the output of `df.schema.json` / `StructType.json`
 *
 * The encoding is detected from the first non-whitespace character (`{` means JSON).
 */
object SparkSchemaUtils {

  /**
   * Read and parse a Spark schema file from a Hadoop-compatible path.
   */
  def loadSchemaFile(path: String, hadoopConf: Configuration): StructType =
    withPartitionAndRound(parseSchema(readFile(path, hadoopConf), path))

  private def readFile(path: String, hadoopConf: Configuration): String = {
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(hadoopConf)
    val inputStream = fs.open(fsPath)
    try {
      new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"))
    } finally {
      inputStream.close()
    }
  }

  private[lakeloader] def parseSchema(content: String, path: String): StructType = {
    val trimmed = content.trim
    require(trimmed.nonEmpty, s"Schema file is empty: $path")

    val parsed =
      try {
        if (trimmed.startsWith("{")) DataType.fromJson(trimmed) else DataType.fromDDL(trimmed)
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"Could not parse Spark schema from $path. Expected either DDL "
              + "('col1 STRING, col2 VARIANT, ...') or Spark schema JSON, i.e. the output of "
              + s"StructType.json starting with a '{'. Cause: ${e.getMessage}",
            e)
      }

    parsed match {
      case st: StructType => st
      case other =>
        throw new IllegalArgumentException(
          s"Schema in $path must be a struct describing the record, got: ${other.simpleString}")
    }
  }

  /**
   * The generator populates `key`, `partition`, `round` and `ts` by name, and the update path reads
   * `partition` and `round` back from the written data. Append the two bookkeeping fields when a
   * user-supplied schema omits them, so custom schemas support upsert rounds without the user
   * having to know about them.
   */
  def withPartitionAndRound(schema: StructType): StructType = {
    val withPartition =
      if (schema.fieldNames.contains("partition")) schema
      else StructType(schema.fields :+ StructField("partition", StringType, nullable = false))

    if (withPartition.fieldNames.contains("round")) withPartition
    else StructType(withPartition.fields :+ StructField("round", IntegerType, nullable = false))
  }
}
