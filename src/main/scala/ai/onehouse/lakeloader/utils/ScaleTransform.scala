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

/**
 * Pure-math transformations used by WorkloadScaler to scale a workload
 * derived by WorkloadSynthesizer up or down along two independent axes:
 * total data volume and total partition count.
 *
 * Kept dependency-free so the scaler and its unit tests do not need Spark
 * or Hudi on the classpath.
 */
object ScaleTransform {

  /**
   * Multiply each per-round record count by `factor`. numRounds is unchanged;
   * this preserves the workload's temporal cadence (bursty commits stay
   * bursty, quiet commits stay quiet). Each result is floored to a Long and
   * clamped to at least 1 record so the generator doesn't emit empty rounds.
   */
  def scaleRecordsPerRound(source: Seq[Long], factor: Double): List[Long] = {
    require(factor > 0.0, s"scale factor must be positive, got $factor")
    source.map(x => math.max(1L, (x * factor).toLong)).toList
  }

  /**
   * Rebuild the leading-non-zero partition-distribution weight vector for a
   * new partition count, preserving the fitted zipf shape observed in the
   * source table.
   *
   *  - If `sourceWeights` is empty, return empty (no distribution info).
   *  - If `targetPartitions <= sourceWeights.size`, truncate to the top N and
   *    re-normalize. The head weights dominate for zipf shape > 0.5, so the
   *    truncation loses little mass.
   *  - If `targetPartitions > sourceWeights.size`, build a fresh
   *    weight vector using `p(rank) = 1 / rank^shape` for rank = 1..target,
   *    normalized to sum=1. This extrapolates the zipf tail beyond what the
   *    source measured.
   *
   * The returned list is sorted descending and re-normalized to sum=1.0.
   */
  def scalePartitionDistribution(
      sourceWeights: Seq[Double],
      targetPartitions: Int,
      fittedShape: Double): List[Double] = {
    require(targetPartitions > 0, s"targetPartitions must be positive, got $targetPartitions")
    if (sourceWeights.isEmpty) return Nil
    if (targetPartitions <= sourceWeights.size) {
      val head = sourceWeights.take(targetPartitions)
      val sum = head.sum
      if (sum <= 0.0) List.fill(targetPartitions)(1.0 / targetPartitions)
      else head.map(_ / sum).toList
    } else {
      // Extrapolate using the fitted zipf shape. If shape=0 (uniform), spread
      // evenly. Otherwise re-derive p(rank) ∝ 1/rank^shape across the new
      // partition count.
      if (fittedShape <= 0.0) List.fill(targetPartitions)(1.0 / targetPartitions)
      else {
        val raw = (1 to targetPartitions).map(r => 1.0 / math.pow(r, fittedShape))
        val sum = raw.sum
        raw.map(_ / sum).toList
      }
    }
  }

  /**
   * Preserve the *fraction* of partitions receiving updates. If the source had
   * `sourceUpdated` out of `sourceTotal` partitions updated, apply that same
   * ratio to the target partition count, ceiling to keep at least the same
   * qualitative behavior (small workloads round up to 1 rather than 0).
   */
  def scaleNumPartitionsToUpdate(
      sourceUpdated: Int,
      sourceTotal: Int,
      targetTotal: Int): Int = {
    if (sourceTotal <= 0 || sourceUpdated <= 0) 0
    else {
      val fraction = sourceUpdated.toDouble / sourceTotal.toDouble
      math.min(targetTotal, math.max(1, math.ceil(fraction * targetTotal).toInt))
    }
  }
}
