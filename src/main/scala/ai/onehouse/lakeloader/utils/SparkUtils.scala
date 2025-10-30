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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkUtils {

  def executeSparkSql(spark: SparkSession, sql: String): Unit = {
    println(s"""
         |Executing:
         |--------------------------------
         |$sql
         |--------------------------------
         |""".stripMargin)

    spark.time {
      spark.sql(sql).show()
    }
  }

  def withPersisted[T](df: DataFrame)(block: => T): T = {
    df.persist(StorageLevel.MEMORY_ONLY)
    try {
      block
    } finally {
      df.unpersist()
    }
  }
}
