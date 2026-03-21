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

object CatalystUtil {
  // Simplified for Spark 4.0 compatibility — internal Catalyst APIs (logicalPlan,
  // Dataset.ofRows) were removed. The per-partition limit optimization is lost but
  // this is only used by ChangeDataGenerator, not the benchmark path.
  def partitionLocalLimit(df: DataFrame, n: Int): DataFrame = {
    df.limit(n)
  }
}