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

import ai.onehouse.lakeloader.utils.BucketRun
import ai.onehouse.lakeloader.utils.BucketRun.{CommitShape, Run, Thresholds}
import org.scalatest.funsuite.AnyFunSuite

class BucketRunSpec extends AnyFunSuite {

  private def shape(inserts: Long, updates: Long,
                    zipf: Double = 0.0, partsUpdated: Int = 0): CommitShape =
    CommitShape(inserts, updates, zipf, partsUpdated)

  ///////////////////////
  // Base cases
  ///////////////////////

  test("detectRuns on empty input returns Nil") {
    assert(BucketRun.detectRuns(Seq.empty).isEmpty)
  }

  test("detectRuns on single commit returns one 1-length run") {
    val runs = BucketRun.detectRuns(Seq(shape(1000, 500)))
    assert(runs.size == 1)
    val r = runs.head
    assert(r.firstCommitIndex == 0)
    assert(r.lastCommitIndex == 0)
    assert(r.size == 1)
    // update ratio = 500 / 1500 ≈ 0.333
    assert(math.abs(r.meanUpdateRatio - 1.0 / 3.0) < 1e-9)
  }

  ///////////////////////
  // Uniform: all commits similar → one run
  ///////////////////////

  test("all-similar commits collapse to a single run") {
    val commits = (1 to 12).map(_ => shape(1000, 100, 1.5, 3))
    val runs = BucketRun.detectRuns(commits)
    assert(runs.size == 1, s"expected 1 run, got ${runs.size}")
    val r = runs.head
    assert(r.firstCommitIndex == 0 && r.lastCommitIndex == 11)
    assert(r.size == 12)
    // update ratio = 100/1100 ≈ 0.0909
    assert(math.abs(r.meanUpdateRatio - 100.0 / 1100.0) < 1e-6)
    assert(r.meanInsertZipfShape == 1.5)
    assert(r.meanPartitionsUpdated == 3)
  }

  ///////////////////////
  // Diurnal: two-phase pattern → two runs
  ///////////////////////

  test("two-phase workload (quiet then busy) produces two runs") {
    // 6 quiet commits: 5% updates, ratio ~0.05
    val quiet = Seq.fill(6)(shape(1000, 50, 1.0, 2))
    // 6 busy commits: 40% updates, ratio ~0.4
    val busy = Seq.fill(6)(shape(1000, 666, 2.5, 8))
    val runs = BucketRun.detectRuns(quiet ++ busy)
    assert(runs.size == 2, s"expected 2 runs, got ${runs.size}: $runs")
    assert(runs(0).size == 6)
    assert(runs(1).size == 6)
    assert(runs(0).meanUpdateRatio < 0.1)
    assert(runs(1).meanUpdateRatio > 0.35)
    // zipf shapes should differentiate too
    assert(runs(0).meanInsertZipfShape == 1.0)
    assert(runs(1).meanInsertZipfShape == 2.5)
  }

  ///////////////////////
  // Alternating: strong flip every commit → many runs
  ///////////////////////

  test("alternating quiet/busy commits produces many small runs") {
    val alternating = (0 until 12).map { i =>
      if (i % 2 == 0) shape(1000, 50, 1.0, 2)
      else shape(1000, 700, 2.5, 8)
    }
    val runs = BucketRun.detectRuns(alternating)
    // No two adjacent commits are similar → 12 runs of size 1
    assert(runs.size == 12, s"expected 12 alternating runs, got ${runs.size}")
    assert(runs.forall(_.size == 1))
  }

  ///////////////////////
  // Records-per-commit variation
  ///////////////////////

  test("commits with very different record counts split into separate runs") {
    // Small commits then large commits
    val small = Seq.fill(4)(shape(100, 10, 1.5, 2))
    val large = Seq.fill(4)(shape(10000, 1000, 1.5, 2)) // records differ 100x
    val runs = BucketRun.detectRuns(small ++ large)
    assert(runs.size == 2, s"expected 2 runs, got ${runs.size}")
    assert(runs(0).meanRecordsPerCommit < 200)
    assert(runs(1).meanRecordsPerCommit > 5000)
  }

  test("commits within 25% record-count variation stay in the same run") {
    // Small oscillations: 1000, 1100, 950, 1050, 900, 1200
    val commits = Seq(1000L, 1100L, 950L, 1050L, 900L, 1200L)
      .map(n => shape(n, (n * 0.1).toLong, 1.5, 3))
    val runs = BucketRun.detectRuns(commits)
    assert(runs.size == 1, s"expected 1 run, got ${runs.size}: $runs")
  }

  ///////////////////////
  // Zipf shape variation
  ///////////////////////

  test("commits with jumping zipf shape split into runs") {
    // 5 commits with zipf=1.0, 5 with zipf=2.5 (delta > 0.3 threshold)
    val flat = Seq.fill(5)(shape(1000, 100, 1.0, 3))
    val skewed = Seq.fill(5)(shape(1000, 100, 2.5, 3))
    val runs = BucketRun.detectRuns(flat ++ skewed)
    assert(runs.size == 2)
    assert(math.abs(runs(0).meanInsertZipfShape - 1.0) < 1e-6)
    assert(math.abs(runs(1).meanInsertZipfShape - 2.5) < 1e-6)
  }

  ///////////////////////
  // Threshold sensitivity
  ///////////////////////

  test("thresholds looser than deltas collapse everything into one run") {
    val commits = Seq(
      shape(1000, 50, 1.0, 2),
      shape(1000, 500, 2.5, 8))
    // Set thresholds huge — anything goes into one run
    val runs = BucketRun.detectRuns(commits,
      Thresholds(updateRatioAbs = 1.0, zipfShapeAbs = 5.0, recordsRelPct = 1.0))
    assert(runs.size == 1, s"expected 1 run with loose thresholds, got ${runs.size}")
  }

  test("thresholds tighter than any delta produce one run per commit") {
    val commits = (0 until 5).map(i => shape(1000, 100 + i * 10L, 1.5 + i * 0.05, 3))
    val runs = BucketRun.detectRuns(commits,
      Thresholds(updateRatioAbs = 0.0001, zipfShapeAbs = 0.001, recordsRelPct = 0.0001))
    assert(runs.size == 5, s"expected 5 singleton runs, got ${runs.size}")
  }

  ///////////////////////
  // expandPerRound
  ///////////////////////

  test("expandPerRound emits one entry per source commit using the run's mean") {
    val commits = Seq.fill(6)(shape(1000, 50, 1.0, 2)) ++
      Seq.fill(4)(shape(1000, 600, 2.5, 8))
    val runs = BucketRun.detectRuns(commits)
    assert(runs.size == 2)

    val perRoundUR = BucketRun.expandPerRound(runs, _.meanUpdateRatio)
    assert(perRoundUR.size == commits.size)
    // first 6 should be quiet ratio; last 4 should be busy ratio
    assert(perRoundUR.take(6).forall(_ < 0.1))
    assert(perRoundUR.drop(6).forall(_ > 0.3))
  }

  test("expandPerRound works with any Run projection") {
    val commits = Seq.fill(3)(shape(500, 50, 1.5, 2))
    val runs = BucketRun.detectRuns(commits)
    val expanded = BucketRun.expandPerRound(runs, _.meanRecordsPerCommit.toLong)
    assert(expanded == List(550L, 550L, 550L))
  }

  ///////////////////////
  // Zero-record commits
  ///////////////////////

  test("commits with zero total records don't crash on ratio compute") {
    val commits = Seq(shape(0, 0, 0.0, 0), shape(1000, 100, 1.0, 2))
    val runs = BucketRun.detectRuns(commits)
    // Zero-record commit has ratio=0, records=0. Second commit has ratio=0.09,
    // records=1100. Records delta = 100% → split.
    assert(runs.size == 2)
  }

  ///////////////////////
  // Drift detection: anchor + end-to-end sanity check
  ///////////////////////

  test("gradual update-ratio drift breaks the run when anchor-delta exceeds 2x threshold") {
    // 30 commits, update-ratio drifts from 0.10 → 0.40 (step ~= 0.01 per commit).
    // Running-mean check absorbs (each step tiny vs mean), but anchor delta
    // eventually exceeds 2x threshold (0.20) and breaks the run.
    val commits = (0 until 30).map { i =>
      val ratio = 0.10 + 0.01 * i
      val ins = ((1.0 - ratio) * 1000).toLong
      val upd = (ratio * 1000).toLong
      shape(ins, upd, 1.0, 3)
    }
    val runs = BucketRun.detectRuns(commits)
    assert(runs.size >= 2,
      s"expected drift to break the run (>=2 runs), got ${runs.size}: $runs")
  }

  test("stable workload with sub-threshold noise stays a single run despite anchor check") {
    // Update-ratio 0.30 ± 0.02 random noise. Neither running-mean nor anchor
    // deltas cross threshold, and end-to-end drift stays under 1x too.
    val rand = new scala.util.Random(seed = 42)
    val commits = (0 until 30).map { _ =>
      val noise = (rand.nextDouble() - 0.5) * 0.04 // ±0.02
      val ratio = math.max(0.0, math.min(1.0, 0.30 + noise))
      val ins = ((1.0 - ratio) * 1000).toLong
      val upd = (ratio * 1000).toLong
      shape(ins, upd, 1.0, 3)
    }
    val runs = BucketRun.detectRuns(commits)
    assert(runs.size == 1,
      s"expected 1 run for noisy-but-stable workload, got ${runs.size}: $runs")
  }

  test("end-to-end sanity check splits when 1 run drifts more than 1x threshold end-to-end") {
    // 6 commits, update-ratio drifts 0.20 → 0.31 (~0.022 per step). Each step
    // and each anchor delta stay under threshold, so the primary walk keeps
    // one run. End-to-end delta = 0.11 > 0.1 threshold → sanity-check splits
    // at midpoint into two 3-commit halves.
    val commits = (0 until 6).map { i =>
      val ratio = 0.20 + 0.022 * i
      val ins = ((1.0 - ratio) * 1000).toLong
      val upd = (ratio * 1000).toLong
      shape(ins, upd, 1.0, 3)
    }
    val runs = BucketRun.detectRuns(commits)
    assert(runs.size == 2,
      s"expected end-to-end sanity check to split (2 runs), got ${runs.size}: $runs")
    assert(runs.head.size == 3)
    assert(runs(1).size == 3)
    // Second half's mean ratio should be higher than the first's.
    assert(runs(1).meanUpdateRatio > runs.head.meanUpdateRatio)
  }

  test("end-to-end sanity check leaves stable workload as one run") {
    // Truly flat workload: no drift end-to-end.
    val commits = (0 until 8).map(_ => shape(1000, 100, 1.0, 3))
    val runs = BucketRun.detectRuns(commits)
    assert(runs.size == 1)
  }
}
