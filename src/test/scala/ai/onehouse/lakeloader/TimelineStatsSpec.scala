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

package ai.onehouse.lakeloader

import ai.onehouse.lakeloader.utils.TimelineStats
import org.scalatest.funsuite.AnyFunSuite

class TimelineStatsSpec extends AnyFunSuite {

  /**
   * Build per-bucket counts for `count(rank) = C / rank^shape`, floor to Long.
   * Uses a large C so quantization noise doesn't dominate the fit.
   */
  private def zipfCounts(nBuckets: Int, shape: Double, scale: Long = 1000000L): Seq[Long] = {
    (1 to nBuckets).map(r => math.max(1L, (scale / math.pow(r, shape)).toLong))
  }

  test("fitZipfShape recovers shape=2.0 within tolerance") {
    val counts = zipfCounts(20, 2.0)
    val fitted = TimelineStats.fitZipfShape(counts)
    assert(math.abs(fitted - 2.0) < 0.05, s"expected ~2.0, got $fitted")
  }

  test("fitZipfShape recovers shape=2.93 within tolerance") {
    val counts = zipfCounts(20, 2.93)
    val fitted = TimelineStats.fitZipfShape(counts)
    assert(math.abs(fitted - 2.93) < 0.1, s"expected ~2.93, got $fitted")
  }

  test("fitZipfShape recovers shape=1.0 within tolerance") {
    val counts = zipfCounts(50, 1.0)
    val fitted = TimelineStats.fitZipfShape(counts)
    assert(math.abs(fitted - 1.0) < 0.05, s"expected ~1.0, got $fitted")
  }

  test("fitZipfShape returns ~0 for uniform counts") {
    val counts = Seq.fill(20)(1000L)
    val fitted = TimelineStats.fitZipfShape(counts)
    assert(fitted < 0.01, s"expected ~0.0 for uniform, got $fitted")
  }

  test("fitZipfShape returns 0 when fewer than two positive counts") {
    assert(TimelineStats.fitZipfShape(Seq.empty) == 0.0)
    assert(TimelineStats.fitZipfShape(Seq(100L)) == 0.0)
    assert(TimelineStats.fitZipfShape(Seq(100L, 0L, 0L)) == 0.0)
  }

  test("fitZipfShape clamps negative slopes to 0 (inverted input)") {
    // Ascending — the source data was sorted the wrong way. Our contract is
    // that inputs are descending; anything else must not produce a negative shape.
    val ascending = (1 to 10).map(i => i.toLong * 100L)
    val fitted = TimelineStats.fitZipfShape(ascending)
    assert(fitted >= 0.0)
  }

  test("median handles odd and even sizes") {
    assert(TimelineStats.median(Seq(1.0, 3.0, 5.0)) == 3.0)
    assert(TimelineStats.median(Seq(1.0, 2.0, 3.0, 4.0)) == 2.5)
    assert(TimelineStats.median(Seq.empty) == 0.0)
  }

  test("medianLong handles odd and even sizes") {
    assert(TimelineStats.medianLong(Seq(1L, 3L, 5L)) == 3L)
    assert(TimelineStats.medianLong(Seq(2L, 4L, 6L, 8L)) == 5L)
    assert(TimelineStats.medianLong(Seq.empty) == 0L)
  }

  test("deriveUpdateRatio averages per-commit ratios ignoring empty commits") {
    val commits = Seq(
      (100L, 0L),    // 0% update
      (50L, 50L),    // 50% update
      (0L, 100L),    // 100% update
      (0L, 0L))      // skipped (no writes)
    val ratio = TimelineStats.deriveUpdateRatio(commits)
    assert(math.abs(ratio - 0.5) < 1e-9, s"expected 0.5, got $ratio")
  }

  test("deriveUpdateRatio returns 0 when all commits are empty") {
    assert(TimelineStats.deriveUpdateRatio(Seq((0L, 0L), (0L, 0L))) == 0.0)
    assert(TimelineStats.deriveUpdateRatio(Seq.empty) == 0.0)
  }

  test("derivePartitionDistribution normalizes and sorts descending") {
    val shares = Map("2025-01-01" -> 100L, "2025-01-02" -> 200L, "2025-01-03" -> 700L)
    val dist = TimelineStats.derivePartitionDistribution(shares)
    assert(dist == List(0.7, 0.2, 0.1))
  }

  test("derivePartitionDistribution drops trailing zeros") {
    val shares = Map("a" -> 10L, "b" -> 0L, "c" -> 90L, "d" -> 0L)
    val dist = TimelineStats.derivePartitionDistribution(shares)
    assert(dist == List(0.9, 0.1))
  }

  test("derivePartitionDistribution returns empty when total is zero") {
    val shares = Map("a" -> 0L, "b" -> 0L)
    assert(TimelineStats.derivePartitionDistribution(shares).isEmpty)
    assert(TimelineStats.derivePartitionDistribution(Map.empty[String, Long]).isEmpty)
  }

  test("distributionsDiffer detects large-enough deviation") {
    val uniform = List.fill(10)(0.1)
    val skewed = List(0.5, 0.3, 0.2)
    assert(TimelineStats.distributionsDiffer(uniform, skewed))
  }

  test("distributionsDiffer ignores tail padding") {
    val a = List(0.5, 0.5)
    val b = List(0.5, 0.5)
    assert(!TimelineStats.distributionsDiffer(a, b))
  }

  test("distributionsDiffer respects epsilon threshold") {
    // within eps=0.05 on every leading entry
    val a = List(0.4, 0.3, 0.3)
    val b = List(0.42, 0.32, 0.26)
    assert(!TimelineStats.distributionsDiffer(a, b, eps = 0.05))
    // over threshold on entry 1
    val c = List(0.4, 0.5, 0.1)
    assert(TimelineStats.distributionsDiffer(a, c, eps = 0.05))
  }
}
