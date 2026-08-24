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
  private val COLLECTION_NULL_PROBABILITY = 0.6

  /** Fixed identity fields set directly by [[generateRow]]/[[attachIdentity]], never generated. */
  val IDENTITY_FIELDS: Set[String] = Set("key", "partition", "round", "ts")

  /**
   * Generate a primary key string per [[KeyType]]. Extracted out of [[generateRow]] so the
   * external-bootstrap datagen path (which stitches identity onto pre-generated payload rows
   * rather than generating a full row at once) can mint new insert keys with the exact same
   * scheme.
   */
  def generateKey(keyType: KeyType, round: Int, ts: Long, random: Random): String =
    keyType match {
      case KeyTypes.TemporallyOrdered =>
        s"${ts}-${UUID.randomUUID()}-${"%03d".format(round)}"
      case KeyTypes.Random =>
        s"${UUID.randomUUID()}-${"%03d".format(round)}"
      case _ => throw new UnsupportedOperationException(s"$keyType not supported")
    }

  /**
   * Append `_<partitionPath>` to a normal [[generateKey]] key, giving
   * `<uuid>-<round:%03d>_<partitionPath>` (or `<ts>-<uuid>-<round>_<partitionPath>` for
   * [[KeyTypes.TemporallyOrdered]]).
   *
   * Used by bootstraps that are later fanned out across partitions by copying one partition's base
   * files verbatim and rewriting only the key suffix (the `ParquetPartitionRewriteJob` flow). That
   * rewriter splits on the *last* underscore, so a key must already carry one for its suffix to be
   * replaced rather than appended. The base key contains no underscore, so the split lands exactly
   * on the separator added here and the whole `<uuid>-<round>` prefix survives the rewrite.
   *
   * The `%03d` round tag is deliberately retained (unlike the externalBootstrap insert-key scheme,
   * which mints a bare `<uuid>_<partition>`): it keeps round-0 keys self-identifying, so a key's
   * originating round is still recoverable after the fan-out.
   *
   * Note the round tag is no longer the last 3 characters, so tests must extract it with a regex
   * (e.g. `regexp_extract(key, "-(\\d{3})_", 1)`) rather than `substring(key, -3, 3)`.
   */
  def partitionSuffixedKey(
      keyType: KeyType,
      round: Int,
      ts: Long,
      random: Random,
      partitionPath: String): String =
    s"${generateKey(keyType, round, ts, random)}_$partitionPath"

  /**
   * Reassemble a full-schema Row from a payload row (i.e. `fullSchema` minus [[IDENTITY_FIELDS]],
   * same field order) plus explicit identity values. Used by the external-bootstrap datagen path:
   * payload data is generated once into a shared pool with identity columns dropped, then sampled
   * rows are re-keyed here for each round/partition without regenerating the payload itself.
   */
  def attachIdentity(
      payloadRow: Row,
      fullSchema: StructType,
      key: String,
      partition: String,
      round: Int,
      ts: Long): Row = {
    val payloadFieldIndex = fullSchema.fields
      .filterNot(f => IDENTITY_FIELDS.contains(f.name))
      .zipWithIndex
      .map { case (f, i) => f.name -> i }
      .toMap
    val values = fullSchema.fields.map { field =>
      field.name match {
        case "key" => key
        case "partition" => partition
        case "round" => round
        case "ts" => ts
        case name => payloadRow.get(payloadFieldIndex(name))
      }
    }
    Row.fromSeq(values)
  }

  /**
   * Generate a Row for the given schema, with special handling for the standard
   * key/partition/round/ts fields, and type-based generation for all other fields.
   *
   * @param nullifyDataFields when true, every field except key/partition/round/ts is set to
   *                          `null` directly (bypassing [[generateValue]] entirely, including for
   *                          array/map fields, which are otherwise never nulled) -- used to
   *                          produce near-empty-on-disk records for partitions that need to exist
   *                          (matching a real table's partition count) but not carry realistic
   *                          data volume.
   * @param suffixKeyWithPartitionPath when true, the normal `keyType` key gets `_<partition>`
   *                          appended (see [[partitionSuffixedKey]]). Applied after the partition
   *                          is sampled, so the suffix always matches the row's own partition.
   */
  def generateRow(
      schema: StructType,
      round: Int,
      partitionPaths: List[String],
      partitionDistributionCDF: List[Double],
      keyType: KeyType,
      recordSize: Int,
      random: Random,
      nullifyDataFields: Boolean = false,
      suffixKeyWithPartitionPath: Boolean = false): Row = {
    val ts = System.currentTimeMillis()
    val partition = partitionPaths(sampleFromCDF(partitionDistributionCDF, random.nextDouble()))
    val key =
      if (suffixKeyWithPartitionPath) partitionSuffixedKey(keyType, round, ts, random, partition)
      else generateKey(keyType, round, ts, random)
    val sizeFactor = Math.max(recordSize / schema.fields.length, 1)

    val values = schema.fields.map { field =>
      field.name match {
        case "key" => key
        case "partition" => partition
        case "round" => round
        case "ts" => ts
        case _ if nullifyDataFields => null
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
        Row.fromSeq(
          st.fields.map(f => generateValue(f.dataType, f.nullable, childSizeFactor, random)))

      case ArrayType(elementType, containsNull) =>
        val size = 1 + random.nextInt(3)
        val childSizeFactor = Math.max(sizeFactor / 4, 1)
        (0 until size)
          .map(_ => generateValue(elementType, containsNull, childSizeFactor, random))
          .toArray

      case MapType(keyType, valueType, valueContainsNull) =>
        val size = 1 + random.nextInt(2)
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
