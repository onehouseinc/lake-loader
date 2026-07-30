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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit, LogicalPlan, Repartition}

object CatalystUtil {
  def partitionLocalLimit(df: DataFrame, n: Int): DataFrame = {
    val originalPlan = df.logicalPlan
    originalPlan match {
      // NOTE: `Dataset.limit` operator is being translated by Spark into:
      //
      //       GlobalLimit(Literal(targetLimit), LocalLimit(Literal(targetLimit), plan))
      //
      //       Where
      //          - `GlobalLimit` has an unfortunate limitation that it will be essentially
      //            shuffling all partitions into a single one prior to executing the limit (even though
      //            it's strictly speaking not required for non-ordered limits), which leads to memory/disk spills
      //            on large enough datasets
      //          - `LocalLimit` even though it will be pushed down, will bear the same target-limit threshold
      //            substantially reducing its effectiveness; consider following case: we're trying to limit
      //            Spark dataset (having 1000 partitions) holding 1B records to hold only 10M records. If we
      //            invoke `df.limit(10M)`, it will result as mentioned above in 1 task collecting all 1B records
      //            before discarding 99.9% of those causing heavy thrashing due to spilling, which will occur
      //            b/c the same global limit (10M) will be applied to every partition
      //
      case Repartition(numPartitions, _, _) =>
        val perPartitionLimit = math.ceil(n / numPartitions).toInt
        val partitionLocalLimitPlan = LocalLimit(Literal(perPartitionLimit), originalPlan)
        Dataset.ofRows(df.sparkSession, partitionLocalLimitPlan)

      case _ => df.limit(n)
    }
  }
}