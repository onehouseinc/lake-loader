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

  def fromString(s: String): OperationType = s match {
    case "upsert" => Upsert
    case "insert" => Insert
    case _ => throw new IllegalArgumentException(s"Invalid OperationType: $s")
  }

  def values(): List[String] = List(Upsert.asString, Insert.asString)
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

  def values(): List[String] = List(Iceberg.asString, Delta.asString, Hudi.asString, Parquet.asString)
}

sealed trait ApiType { def asString: String }

object ApiType {
  case object SparkDatasourceApi extends ApiType { val asString = "spark-datasource" }
  case object SparkSqlApi extends ApiType { val asString = "spark-sql" }
}

case class LoadConfig(numberOfRounds: Int = 10,
                      inputPath: String = "",
                      outputPath: String = "",
                      parallelism: Int = 100,
                      format: String = "hudi",
                      operationType: String = "upsert",
                      options: Map[String, String] = Map.empty,
                      nonPartitioned: Boolean = false,
                      experimentId: String = StringUtils.generateRandomString(10),
                      startRound: Int = 0,
                      catalog: String = "spark_catalog",
                      database: String = "default",
                      mergeMode: String = "update-insert",
                      additionalMergeConditionColumns: Seq[String] = Seq.empty,
                      updateColumns: Seq[String] = Seq.empty)
