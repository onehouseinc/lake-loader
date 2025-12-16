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

package ai.onehouse.lakeloader.configs

import ai.onehouse.lakeloader.utils.StringUtils

sealed trait OperationType {
  def asString: String
}

object OperationType {
  case object Upsert extends OperationType { val asString = "upsert" }
  case object Insert extends OperationType { val asString = "insert" }
  case object BulkInsert extends OperationType { val asString = "bulk_insert" }

  def fromString(s: String): OperationType = s match {
    case "upsert" => Upsert
    case "insert" => Insert
    case "bulk_insert" => BulkInsert
    case _ => throw new IllegalArgumentException(s"Invalid OperationType: $s")
  }

  def values(): List[String] = List(Upsert.asString, Insert.asString, BulkInsert.asString)

  implicit val operationTypeRead: scopt.Read[OperationType] = scopt.Read.reads { s =>
    try {
      OperationType.fromString(s)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Invalid operation type: $s. Valid values: ${OperationType.values().mkString(", ")}")
    }
  }
}

sealed trait MergeMode {
  def asString: String
}

object MergeMode {
  case object UpdateInsert extends MergeMode { val asString = "update-insert" }
  case object DeleteInsert extends MergeMode { val asString = "delete-insert" }

  def fromString(s: String): MergeMode = s match {
    case "update-insert" => UpdateInsert
    case "delete-insert" => DeleteInsert
    case _ => throw new IllegalArgumentException(s"Invalid MergeMode: $s")
  }

  def values(): List[String] = List(UpdateInsert.asString, DeleteInsert.asString)

  implicit val mergeModeRead: scopt.Read[MergeMode] = scopt.Read.reads { s =>
    try {
      MergeMode.fromString(s)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Invalid merge mode: $s. Valid values: ${MergeMode.values().mkString(", ")}")
    }
  }
}

sealed trait StorageFormat {
  def asString: String
}

object StorageFormat {
  case object Iceberg extends StorageFormat { val asString = "iceberg" }
  case object Delta extends StorageFormat { val asString = "delta" }
  case object Hudi extends StorageFormat { val asString = "hudi" }
  case object Parquet extends StorageFormat { val asString = "parquet" }

  def fromString(s: String): StorageFormat = s match {
    case "iceberg" => Iceberg
    case "delta" => Delta
    case "hudi" => Hudi
    case "parquet" => Parquet
    case _ => throw new IllegalArgumentException(s"Invalid StorageFormat: $s")
  }

  def values(): List[String] =
    List(Iceberg.asString, Delta.asString, Hudi.asString, Parquet.asString)

  implicit val storageFormatRead: scopt.Read[StorageFormat] = scopt.Read.reads { s =>
    try {
      StorageFormat.fromString(s)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Invalid storage format: $s. Valid values: ${StorageFormat.values().mkString(", ")}")
    }
  }
}

sealed trait ApiType { def asString: String }

object ApiType {
  case object SparkDatasourceApi extends ApiType { val asString = "spark-datasource" }
  case object SparkSqlApi extends ApiType { val asString = "spark-sql" }

  def fromString(s : String): ApiType = s match {
    case "spark-datasource" => SparkDatasourceApi
    case "spark-sql" => SparkSqlApi
    case _ => throw new IllegalArgumentException(s"Invalid ApiType: $s")
  }

  def values(): List[String] =
    List(SparkDatasourceApi.asString, SparkSqlApi.asString)

  implicit val apiTypeRead: scopt.Read[ApiType] = scopt.Read.reads { s =>
    try {
      ApiType.fromString(s)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Invalid api Type: $s. Valid values: ${ApiType.values().mkString(", ")}")
    }
  }
}

case class LoadConfig(
    numberOfRounds: Int = 10,
    inputPath: String = "",
    outputPath: String = "",
    format: String = "hudi",
    initialOperationType: String = "bulk_insert",
    operationType: String = "upsert",
    apiType: String = "spark-datasource",
    options: Map[String, String] = Map.empty,
    nonPartitioned: Boolean = false,
    experimentId: String = StringUtils.generateRandomString(10),
    startRound: Int = 0,
    catalog: String = "spark_catalog",
    database: String = "default",
    mergeMode: String = "update-insert",
    additionalMergeConditionColumns: Seq[String] = Seq.empty,
    updateColumns: Seq[String] = Seq.empty)
