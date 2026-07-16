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
   *      records. Compare against both:
   *        (a) the *running mean* of the current buffer (soft check —
   *            catches abrupt shifts);
   *        (b) the *first-commit anchor* of the current run (hard check —
   *            catches gradual drift). Anchor deltas must be within
   *            2 × threshold on each axis; otherwise the run breaks.
   *   3. If both checks pass, extend the run. Otherwise close the current run
   *      and start a new one at this commit, re-anchoring at that commit.
   *   4. When done, emit runs with their aggregated mean stats.
   *
   * The anchor check is what distinguishes this from a naive running-mean
   * classifier: without it, a workload that gradually drifts (say
   * update-ratio 0.3 → 0.5 across 30 commits) would absorb into one run
   * because each step is well under threshold against the accumulating
   * mean. The anchor check breaks the run once total drift exceeds 2 × the
   * per-step tolerance, so gradual regime shifts become detectable.
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
    // First-commit anchor stats. Retained alongside running means so we can
    // detect *gradual drift*: without this, a slowly-shifting workload can
    // absorb into a single run because each next commit is only a tiny step
    // above the running mean, but the run's endpoints are far apart. We break
    // the run when the next commit deviates from the anchor by more than
    // 2 × threshold — one run-width of drift is enough to declare a new run.
    var anchorUR = commitUpdateRatio(commits(0))
    var anchorRecs = commitRecords(commits(0)).toDouble
    var anchorZipf = commits(0).insertZipfShape

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

      // Anchor deltas: how far this commit has drifted from where the run
      // started. Any of the three exceeding 2 × threshold breaks the run
      // even if the running-mean check would have absorbed it.
      val urAnchorDelta = math.abs(ur - anchorUR)
      val zipfAnchorDelta = math.abs(c.insertZipfShape - anchorZipf)
      val recsAnchorDelta =
        if (math.max(recs, anchorRecs) == 0.0) 0.0
        else math.abs(recs - anchorRecs) / math.max(recs, anchorRecs)

      val withinMean =
        urDelta <= thresholds.updateRatioAbs &&
        zipfDelta <= thresholds.zipfShapeAbs &&
        recsDelta <= thresholds.recordsRelPct
      val withinAnchor =
        urAnchorDelta <= 2.0 * thresholds.updateRatioAbs &&
        zipfAnchorDelta <= 2.0 * thresholds.zipfShapeAbs &&
        recsAnchorDelta <= 2.0 * thresholds.recordsRelPct
      val sameRun = withinMean && withinAnchor

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
        // Re-anchor at the new run's first commit.
        anchorUR = ur
        anchorRecs = recs
        anchorZipf = c.insertZipfShape
      }
    }
    flushRun()

    // Second-pass sanity check: if the primary walk collapsed everything into
    // a single run, cross-check the first commit against the last. The
    // per-step anchor uses 2 × threshold to allow one run-width of natural
    // drift, so it can absorb a workload whose end-to-end drift is between
    // 1 × and 2 × threshold. We want to catch that as "yes there's a shift"
    // and split into two runs at the midpoint.
    if (runs.size == 1 && commits.size >= 3) {
      val first = commits.head
      val last = commits.last
      val firstUR = commitUpdateRatio(first)
      val lastUR = commitUpdateRatio(last)
      val firstRecs = commitRecords(first).toDouble
      val lastRecs = commitRecords(last).toDouble
      val urEnds = math.abs(lastUR - firstUR)
      val zipfEnds = math.abs(last.insertZipfShape - first.insertZipfShape)
      val recsEnds =
        if (math.max(firstRecs, lastRecs) == 0.0) 0.0
        else math.abs(lastRecs - firstRecs) / math.max(firstRecs, lastRecs)

      val endToEndShifted =
        urEnds > thresholds.updateRatioAbs ||
        zipfEnds > thresholds.zipfShapeAbs ||
        recsEnds > thresholds.recordsRelPct

      if (endToEndShifted) return splitSingleRunAtMidpoint(commits, runs.head)
    }

    runs.toList
  }

  /** Break a single collapsed run into two halves at the midpoint index,
    * recomputing each half's aggregated means. Called only when the sanity
    * check in detectRuns finds an end-to-end drift larger than one threshold
    * despite the primary walk producing a single run.
    */
  private def splitSingleRunAtMidpoint(
      commits: Seq[CommitShape],
      original: Run): List[Run] = {
    def commitUpdateRatio(c: CommitShape): Double = {
      val total = c.inserts + c.updates
      if (total <= 0) 0.0 else c.updates.toDouble / total.toDouble
    }
    def commitRecords(c: CommitShape): Long = c.inserts + c.updates

    val n = original.size
    val midOffset = n / 2 // integer division; first half has n/2 entries, second has n - n/2
    val firstStart = original.firstCommitIndex
    val firstEnd = firstStart + midOffset - 1
    val secondStart = firstEnd + 1
    val secondEnd = original.lastCommitIndex

    def buildRun(startIdx: Int, endIdx: Int): Run = {
      val slice = commits.slice(startIdx, endIdx + 1)
      val sz = slice.size
      val meanUR = slice.map(commitUpdateRatio).sum / sz
      val meanRecs = slice.map(c => commitRecords(c).toDouble).sum / sz
      val meanZipf = slice.map(_.insertZipfShape).sum / sz
      val meanPart = math.max(1,
        math.round(slice.map(_.numPartitionsWithUpdates.toDouble).sum / sz).toInt)
      Run(startIdx, endIdx, meanUR, meanRecs, meanZipf, meanPart)
    }
    List(buildRun(firstStart, firstEnd), buildRun(secondStart, secondEnd))
  }

  /** Expand runs back to a per-round value list by taking each run's mean value
    * and repeating it for every commit in that run. Length equals the total
    * number of commits across all runs.
    */
  def expandPerRound[T](runs: List[Run], mean: Run => T): List[T] = {
    runs.flatMap(r => List.fill(r.size)(mean(r)))
  }
}
