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

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.avro.HoodieSparkAvroSchemaConverters
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors

object AvroSchemaUtils {

  /**
   * Parse an Avro schema (.avsc) file from a Hadoop-compatible path.
   */
  def parseAvroSchemaFile(path: String, hadoopConf: Configuration): Schema = {
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(hadoopConf)
    val inputStream = fs.open(fsPath)
    try {
      val content = new BufferedReader(new InputStreamReader(inputStream))
        .lines()
        .collect(Collectors.joining("\n"))
      new Schema.Parser().parse(content)
    } finally {
      inputStream.close()
    }
  }

  /**
   * Convert an Avro Schema to a Spark StructType using Hudi's HoodieSparkAvroSchemaConverters,
   * appending 'partition' and 'round' fields if they are not already present in the schema.
   */
  def avroSchemaToSparkSchema(avroSchema: Schema): StructType = {
    val (dataType, _) = HoodieSparkAvroSchemaConverters.toSqlType(avroSchema)
    val sparkSchema = dataType.asInstanceOf[StructType]
    val fieldNames = sparkSchema.fieldNames.toSet

    val withPartition = if (!fieldNames.contains("partition")) {
      StructType(sparkSchema.fields :+ StructField("partition", StringType, nullable = false))
    } else {
      sparkSchema
    }

    if (!withPartition.fieldNames.contains("round")) {
      StructType(withPartition.fields :+ StructField("round", IntegerType, nullable = false))
    } else {
      withPartition
    }
  }

  /**
   * Full pipeline: parse .avsc file, convert to Spark StructType, and augment with partition/round.
   */
  def loadSchemaFromAvscFile(path: String, hadoopConf: Configuration): StructType = {
    val avroSchema = parseAvroSchemaFile(path, hadoopConf)
    avroSchemaToSparkSchema(avroSchema)
  }
}
