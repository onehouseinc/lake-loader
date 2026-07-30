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

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors
import scala.collection.JavaConverters._

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
   * Convert an Avro Schema to a Spark StructType, appending 'partition' and 'round' fields
   * if they are not already present in the schema.
   */
  def avroSchemaToSparkSchema(avroSchema: Schema): StructType = {
    val sparkSchema = avroToSparkType(avroSchema).asInstanceOf[StructType]
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

  private def isNullable(schema: Schema): Boolean =
    schema.getType == NULL ||
      (schema.getType == UNION && schema.getTypes.asScala.exists(_.getType == NULL))

  private def avroToSparkType(schema: Schema): DataType = schema.getType match {
    case STRING => StringType
    case BOOLEAN => BooleanType
    case FLOAT => FloatType
    case DOUBLE => DoubleType
    case BYTES => BinaryType
    case NULL => NullType
    case ENUM => StringType

    case INT =>
      Option(schema.getLogicalType) match {
        case Some(lt) if lt.getName == "date" => DateType
        case _ => IntegerType
      }

    case LONG =>
      Option(schema.getLogicalType) match {
        case Some(lt)
            if lt.getName == "timestamp-millis" || lt.getName == "local-timestamp-millis" =>
          TimestampType
        case Some(lt)
            if lt.getName == "timestamp-micros" || lt.getName == "local-timestamp-micros" =>
          TimestampType
        case _ => LongType
      }

    case FIXED =>
      Option(schema.getLogicalType) match {
        case Some(lt: LogicalTypes.Decimal) => DecimalType(lt.getPrecision, lt.getScale)
        case _ => BinaryType
      }

    case RECORD =>
      val fields = schema.getFields.asScala.map { field =>
        StructField(
          field.name(),
          avroToSparkType(field.schema()),
          nullable = isNullable(field.schema()))
      }
      StructType(fields.toSeq)

    case ARRAY =>
      ArrayType(
        avroToSparkType(schema.getElementType),
        containsNull = isNullable(schema.getElementType))

    case MAP =>
      MapType(
        StringType,
        avroToSparkType(schema.getValueType),
        valueContainsNull = isNullable(schema.getValueType))

    case UNION =>
      val nonNullTypes = schema.getTypes.asScala.filter(_.getType != NULL)
      if (nonNullTypes.size == 1) {
        avroToSparkType(nonNullTypes.head)
      } else {
        StringType // multiple non-null branches — fall back to string
      }

    case other =>
      throw new UnsupportedOperationException(s"Unsupported Avro type: $other")
  }
}
