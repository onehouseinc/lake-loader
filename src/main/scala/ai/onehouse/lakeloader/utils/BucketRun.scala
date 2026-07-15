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
 * Positional bucketing over a sequence of commit summaries: walk the sequence
 * left-to-right and group adjacent commits with similar characteristics into
 * runs. Each run then carries a mean set of parameters that the WorkloadResizer
 * emits as per-round values, reproducing the source workload's burstiness
 * (e.g. quiet overnight commits followed by heavier business-hour commits).
 *
 * Pure math — no Spark, no Hudi, no I/O.
 */
object BucketRun {

  /** A single commit's shape, minimal fields needed for run detection. */
  final case class CommitShape(
      inserts: Long,
      updates: Long,
      insertZipfShape: Double,
      numPartitionsWithUpdates: Int)

  /** One detected run of similar commits: an index range plus mean stats. */
  final case class Run(
      firstCommitIndex: Int,       // inclusive
      lastCommitIndex: Int,        // inclusive
      meanUpdateRatio: Double,     // avg updates / (inserts + updates) over the run
      meanRecordsPerCommit: Double, // avg (inserts + updates) over the run
      meanInsertZipfShape: Double,
      meanPartitionsUpdated: Int) {
    def size: Int = lastCommitIndex - firstCommitIndex + 1
  }

  /** Thresholds that decide whether two adjacent commits belong to the same run. */
  final case class Thresholds(
      updateRatioAbs: Double = 0.1,   // |Δupdate-ratio| tolerance
      zipfShapeAbs: Double = 0.3,     // |Δzipf-shape| tolerance
      recordsRelPct: Double = 0.25)   // |Δrecords-per-commit| / max(a,b) tolerance

  /**
   * Detect adjacent-run structure in `commits`.
   *
   * Algorithm:
   *   1. Walk commits left to right maintaining a current-run buffer.
   *   2. For each next commit, compute its update-ratio, insert-zipf, and total
   *      records. Compare against the *running mean* of the current buffer.
   *   3. If all three deltas are within thresholds, extend the run. Otherwise,
   *      close the current run and start a new one at this commit.
   *   4. When done, emit runs with their aggregated mean stats.
   *
   * Comparing against running mean (rather than the previous single commit)
   * keeps runs from drifting arbitrarily: if commits gradually shift, a run
   * eventually deviates enough from its cumulative mean to break.
   *
   * Empty input returns Nil. Single commit returns one 1-length run.
   */
  def detectRuns(
      commits: Seq[CommitShape],
      thresholds: Thresholds = Thresholds()): List[Run] = {
    if (commits.isEmpty) return Nil

    def commitUpdateRatio(c: CommitShape): Double = {
      val total = c.inserts + c.updates
      if (total <= 0) 0.0 else c.updates.toDouble / total.toDouble
    }
    def commitRecords(c: CommitShape): Long = c.inserts + c.updates

    val runs = scala.collection.mutable.ListBuffer[Run]()
    val currentIdxs = scala.collection.mutable.ArrayBuffer[Int](0)
    var sumUR = commitUpdateRatio(commits(0))
    var sumRecs = commitRecords(commits(0)).toDouble
    var sumZipf = commits(0).insertZipfShape
    var sumPart = commits(0).numPartitionsWithUpdates.toDouble

    def flushRun(): Unit = {
      val n = currentIdxs.size
      runs += Run(
        firstCommitIndex = currentIdxs.head,
        lastCommitIndex = currentIdxs.last,
        meanUpdateRatio = sumUR / n,
        meanRecordsPerCommit = sumRecs / n,
        meanInsertZipfShape = sumZipf / n,
        // math.round is HALF_UP: 2.5 -> 3. Min-clamped to 1 so a run's
        // "no partitions updated" (all-insert commits) still gives us a
        // usable value downstream.
        meanPartitionsUpdated = math.max(1, math.round(sumPart / n).toInt))
    }

    (1 until commits.size).foreach { i =>
      val n = currentIdxs.size
      val meanUR = sumUR / n
      val meanRecs = sumRecs / n
      val meanZipf = sumZipf / n
      val c = commits(i)
      val ur = commitUpdateRatio(c)
      val recs = commitRecords(c).toDouble
      val urDelta = math.abs(ur - meanUR)
      val zipfDelta = math.abs(c.insertZipfShape - meanZipf)
      val recsDelta =
        if (math.max(recs, meanRecs) == 0.0) 0.0
        else math.abs(recs - meanRecs) / math.max(recs, meanRecs)

      val sameRun =
        urDelta <= thresholds.updateRatioAbs &&
        zipfDelta <= thresholds.zipfShapeAbs &&
        recsDelta <= thresholds.recordsRelPct

      if (sameRun) {
        currentIdxs += i
        sumUR += ur
        sumRecs += recs
        sumZipf += c.insertZipfShape
        sumPart += c.numPartitionsWithUpdates.toDouble
      } else {
        flushRun()
        currentIdxs.clear()
        currentIdxs += i
        sumUR = ur
        sumRecs = recs
        sumZipf = c.insertZipfShape
        sumPart = c.numPartitionsWithUpdates.toDouble
      }
    }
    flushRun()
    runs.toList
  }

  /** Expand runs back to a per-round value list by taking each run's mean value
    * and repeating it for every commit in that run. Length equals the total
    * number of commits across all runs.
    */
  def expandPerRound[T](runs: List[Run], mean: Run => T): List[T] = {
    runs.flatMap(r => List.fill(r.size)(mean(r)))
  }
}
