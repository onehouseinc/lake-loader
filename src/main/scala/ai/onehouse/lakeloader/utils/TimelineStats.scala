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
 * Pure-math derivations used by WorkloadSynthesizer to turn per-commit,
 * per-partition write counts into ChangeDataGenerator config values. Kept
 * dependency-free (no Spark, no Hudi) so it can be exercised with plain
 * Scala unit tests.
 */
object TimelineStats {

  /**
   * Fit a Zipfian shape parameter s (from p(rank) ∝ 1/rank^s) to a
   * descending-sorted vector of per-bucket counts via OLS regression on
   * log(count) vs log(rank). The negative slope is s.
   *
   * Returns 0.0 when there are fewer than two positive counts (no skew signal).
   * Buckets with zero counts are dropped before the fit — zeros are -inf in log
   * space and would poison the regression.
   */
  def fitZipfShape(sortedDescCounts: Seq[Long]): Double = {
    val points = sortedDescCounts.zipWithIndex.collect {
      case (c, i) if c > 0L => (math.log((i + 1).toDouble), math.log(c.toDouble))
    }
    if (points.size < 2) return 0.0
    val n = points.size
    val sumX = points.map(_._1).sum
    val sumY = points.map(_._2).sum
    val meanX = sumX / n
    val meanY = sumY / n
    var num = 0.0
    var den = 0.0
    points.foreach { case (x, y) =>
      num += (x - meanX) * (y - meanY)
      den += (x - meanX) * (x - meanX)
    }
    if (den == 0.0) 0.0 else math.max(0.0, -num / den)
  }

  /**
   * Median of a sequence of doubles. Empty input returns 0.0.
   */
  def median(xs: Seq[Double]): Double = {
    if (xs.isEmpty) return 0.0
    val sorted = xs.sorted
    val n = sorted.size
    if (n % 2 == 1) sorted(n / 2)
    else (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0
  }

  def medianLong(xs: Seq[Long]): Long = {
    if (xs.isEmpty) return 0L
    val sorted = xs.sorted
    val n = sorted.size
    if (n % 2 == 1) sorted(n / 2)
    else (sorted(n / 2 - 1) + sorted(n / 2)) / 2L
  }

  /**
   * Given per-commit insert-vs-update counts, return the mean update ratio
   * across commits that had at least one write. Round 0 of the source table
   * is typically all inserts; that's fine, it just averages down.
   */
  def deriveUpdateRatio(perCommitInsertsUpdates: Seq[(Long, Long)]): Double = {
    val ratios = perCommitInsertsUpdates.collect {
      case (ins, upd) if ins + upd > 0 => upd.toDouble / (ins + upd).toDouble
    }
    if (ratios.isEmpty) 0.0 else ratios.sum / ratios.size
  }

  /**
   * Take a map of partition→insert-count summed across commits, normalize it,
   * sort descending, and return the leading non-zero weights. The result is
   * suitable for lake-loader's `--partition-distribution` flag; trailing zeros
   * are dropped because ChangeDataGenerator zero-pads to `totalPartitions`.
   */
  def derivePartitionDistribution(partitionInsertShares: Map[String, Long]): List[Double] = {
    val total = partitionInsertShares.values.sum
    if (total == 0L) return Nil
    partitionInsertShares.values.toList
      .map(_.toDouble / total.toDouble)
      .sorted(Ordering[Double].reverse)
      .takeWhile(_ > 0.0)
  }

  /**
   * True if two normalized weight vectors are materially different — used to
   * decide whether to emit the two-segment `first;subsequent` form of the
   * partition-distribution flag. Compares only the leading `k` entries; the
   * tails are irrelevant since they're zero-padded downstream.
   */
  def distributionsDiffer(a: Seq[Double], b: Seq[Double], eps: Double = 0.05): Boolean = {
    val k = math.max(a.size, b.size)
    (0 until k).exists { i =>
      val av = if (i < a.size) a(i) else 0.0
      val bv = if (i < b.size) b(i) else 0.0
      math.abs(av - bv) > eps
    }
  }
}
