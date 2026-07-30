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

import ai.onehouse.lakeloader.configs.KeyTypes
import ai.onehouse.lakeloader.configs.KeyTypes.KeyType
import ai.onehouse.lakeloader.utils.MathUtils.sampleFromCDF
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.LocalDate
import java.util.UUID
import scala.util.Random

object ComplexDataGenerator extends Serializable {

  private val NULL_PROBABILITY = 0.2
  private val COLLECTION_NULL_PROBABILITY = 0.4

  /**
   * Generate a Row for the given schema, with special handling for the standard
   * key/partition/round/ts fields, and type-based generation for all other fields.
   */
  def generateRow(
      schema: StructType,
      round: Int,
      partitionPaths: List[String],
      partitionDistributionCDF: List[Double],
      keyType: KeyType,
      recordSize: Int,
      random: Random): Row = {
    val ts = System.currentTimeMillis()
    val key = keyType match {
      case KeyTypes.TemporallyOrdered =>
        s"${ts}-${UUID.randomUUID()}-${"%03d".format(round)}"
      case KeyTypes.Random =>
        s"${UUID.randomUUID()}-${"%03d".format(round)}"
      case _ => throw new UnsupportedOperationException(s"$keyType not supported")
    }
    val partition = partitionPaths(sampleFromCDF(partitionDistributionCDF, random.nextDouble()))
    val sizeFactor = Math.max(recordSize / schema.fields.length, 1)

    val values = schema.fields.map { field =>
      field.name match {
        case "key" => key
        case "partition" => partition
        case "round" => round
        case "ts" => ts
        case _ => generateValue(field.dataType, field.nullable, sizeFactor, random)
      }
    }
    Row.fromSeq(values)
  }

  /**
   * Recursively generate a random value for the given Spark DataType.
   */
  def generateValue(dataType: DataType, nullable: Boolean, sizeFactor: Int, random: Random): Any = {
    val nullProbability = dataType match {
      case ArrayType(_, _) | MapType(_, _, _) => COLLECTION_NULL_PROBABILITY
      case _ => NULL_PROBABILITY
    }
    if (nullable && random.nextDouble() < nullProbability) {
      // Arrays/maps: emit an empty collection rather than null. A null container here (as
      // opposed to a genuinely absent/optional leaf value) triggered a parquet-avro schema
      // mismatch during Hudi upserts (ClassCastException: "... is not a group") when merging
      // update-round files against bootstrap-round files -- an empty collection still carries
      // the full nested Avro/Parquet schema for the field, so readers never disagree on its type.
      return dataType match {
        case ArrayType(_, _) => Array.empty[Any]
        case MapType(_, _, _) => Map.empty[Any, Any]
        case _ => null
      }
    }

    dataType match {
      case StringType =>
        StringUtils.generateRandomString(
          Math.max(sizeFactor + random.nextInt(Math.max(sizeFactor, 1)), 1),
          random)

      case IntegerType =>
        random.nextInt()

      case LongType =>
        random.nextLong()

      case FloatType =>
        random.nextFloat()

      case DoubleType =>
        random.nextDouble()

      case BooleanType =>
        random.nextBoolean()

      case BinaryType =>
        val bytes = new Array[Byte](Math.max(sizeFactor, 1))
        random.nextBytes(bytes)
        bytes

      case dt: DecimalType =>
        val scale = dt.scale
        val unscaledMax = BigInt(10).pow(dt.precision) - 1
        val bits = (unscaledMax.bitLength + 1).min(63)
        val unscaled = (BigInt(bits, random) % (unscaledMax + 1)).abs
        new java.math.BigDecimal(unscaled.bigInteger, scale)

      case DateType =>
        Date.valueOf(LocalDate.now().minusDays(random.nextInt(365).toLong))

      case TimestampType =>
        new Timestamp(System.currentTimeMillis() - (random.nextInt(365 * 24 * 3600).toLong * 1000L))

      case st: StructType =>
        val childSizeFactor = Math.max(sizeFactor / Math.max(st.fields.length, 1), 1)
        Row.fromSeq(st.fields.map(f => generateValue(f.dataType, f.nullable, childSizeFactor, random)))

      case ArrayType(elementType, containsNull) =>
        val size = 1 + random.nextInt(4)
        val childSizeFactor = Math.max(sizeFactor / 4, 1)
        (0 until size).map(_ => generateValue(elementType, containsNull, childSizeFactor, random)).toArray

      case MapType(keyType, valueType, valueContainsNull) =>
        val size = 1 + random.nextInt(3)
        val childSizeFactor = Math.max(sizeFactor / 4, 1)
        (0 until size).map { _ =>
          val k = keyType match {
            case StringType => StringUtils.generateRandomString(8, random)
            case _ => generateValue(keyType, nullable = false, childSizeFactor, random)
          }
          k -> generateValue(valueType, valueContainsNull, childSizeFactor, random)
        }.toMap

      case NullType =>
        null

      case other =>
        throw new UnsupportedOperationException(s"Unsupported data type for generation: $other")
    }
  }
}
