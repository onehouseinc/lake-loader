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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}

/**
 * Spark 4.x access to the logical plan of a [[DataFrame]] and back. Since Spark 4.0,
 * `org.apache.spark.sql.Dataset` is an abstract API and the plan-level members live on
 * the classic implementation.
 */
object DatasetShim {
  def logicalPlan(df: DataFrame): LogicalPlan = df.asInstanceOf[ClassicDataset[Row]].logicalPlan

  def ofRows(spark: SparkSession, plan: LogicalPlan): DataFrame =
    ClassicDataset.ofRows(spark.asInstanceOf[ClassicSparkSession], plan)
}
