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
 * Pure-math rollups used by WriteProfiler to turn per-file-group write records
 * into a description of how a table is actually being written. Kept
 * dependency-free (no Spark, no Hudi) so it can be exercised with plain Scala
 * unit tests.
 *
 * The central quantity here is **write amplification**: `numWrites` on a
 * HoodieWriteStat is the record count of the file the write produced, while
 * `numInserts + numUpdates + numDeletes` is the count the write actually
 * contributed. When Hudi appends a small batch into an existing base file it
 * rewrites the whole file, so the former can be many times the latter. A
 * workload description that reports only the latter understates how much IO the
 * table performs; one that reports only the former overstates the workload.
 */
object WriteProfileStats {

  /** Minimal per-file-group write record. Mirrors WorkloadSynthesizer.FileGroupWrite. */
  case class Write(
      fileId: String,
      partitionPath: String,
      instant: String,
      created: Boolean,
      isBaseFile: Boolean,
      numWrites: Long,
      numInserts: Long,
      numUpdates: Long,
      numDeletes: Long,
      totalWriteBytes: Long,
      fileSizeInBytes: Long) {

    /** Records this write contributed, as opposed to records it rewrote. */
    def newRecords: Long = numInserts + numUpdates + numDeletes
  }

  /** One file group's history across the analyzed window. */
  case class FileGroupProfile(
      fileId: String,
      partitionPath: String,
      touches: Int,
      createdInWindow: Boolean,
      firstInstant: String,
      lastInstant: String,
      // numWrites at the first/last base-file write seen. Only meaningful when
      // baseFileTouches > 0; log appends report their own block count, not the
      // file group total, so they can't answer "how big is this group now".
      recordsAtFirstBaseWrite: Option[Long],
      recordsAtLastBaseWrite: Option[Long],
      baseFileTouches: Int,
      inserts: Long,
      updates: Long,
      deletes: Long,
      recordsWritten: Long,
      bytesWritten: Long) {
    def newRecords: Long = inserts + updates + deletes
    def amplification: Option[Double] =
      if (newRecords > 0) Some(recordsWritten.toDouble / newRecords.toDouble) else None
    def hasInserts: Boolean = inserts > 0
    def hasUpdates: Boolean = updates > 0
    def hasDeletes: Boolean = deletes > 0
  }

  /**
   * Roll a flat list of writes up by file group, preserving first/last ordering
   * by instant so growth within the window can be read off.
   */
  def profileFileGroups(writes: Seq[Write]): List[FileGroupProfile] = {
    writes
      .groupBy(_.fileId)
      .toList
      .map { case (fileId, ws) =>
        val ordered = ws.sortBy(_.instant)
        val baseWrites = ordered.filter(_.isBaseFile)
        FileGroupProfile(
          fileId = fileId,
          partitionPath = ordered.head.partitionPath,
          touches = ordered.size,
          createdInWindow = ordered.exists(_.created),
          firstInstant = ordered.head.instant,
          lastInstant = ordered.last.instant,
          recordsAtFirstBaseWrite = baseWrites.headOption.map(_.numWrites),
          recordsAtLastBaseWrite = baseWrites.lastOption.map(_.numWrites),
          baseFileTouches = baseWrites.size,
          inserts = ordered.map(_.numInserts).sum,
          updates = ordered.map(_.numUpdates).sum,
          deletes = ordered.map(_.numDeletes).sum,
          recordsWritten = ordered.map(_.numWrites).sum,
          bytesWritten = ordered.map(_.totalWriteBytes).sum)
      }
      .sortBy(fg => (-fg.recordsWritten, fg.fileId))
  }

  /** Aggregate view of one population of commits (ingest, or table service). */
  case class SegmentSummary(
      commits: Int,
      partitionsTouched: Int,
      fileGroupsTouched: Int,
      fileGroupsCreated: Int,
      fileGroupsRewritten: Int,
      fileGroupsWithInserts: Int,
      fileGroupsWithUpdates: Int,
      fileGroupsWithDeletes: Int,
      inserts: Long,
      updates: Long,
      deletes: Long,
      recordsWritten: Long,
      bytesWritten: Long,
      // Fraction of contributed records that were updates. Distinct from
      // amplification, which is about rewritten records.
      updateShareOfNewRecords: Double,
      // Records written per record contributed, ACROSS THIS WINDOW ONLY. Not an
      // intrinsic property of the table: each time a file group is rewritten its
      // whole record count is counted again, so a longer window yields a larger
      // figure for the same table (measured 7.5x over 60 commits and 28.2x over
      // 721 commits on the same source). Read it as a cost rate for the window
      // -- "to land N new records over this span, Hudi wrote M" -- and never
      // compare values taken over windows of different length.
      writeAmplification: Double,
      bytesPerRecordWritten: Double,
      bytesPerNewRecord: Double,
      medianTouchesPerFileGroup: Double,
      medianAmplificationPerFileGroup: Double)

  def summarize(commits: Int, groups: Seq[FileGroupProfile]): SegmentSummary = {
    val inserts = groups.map(_.inserts).sum
    val updates = groups.map(_.updates).sum
    val deletes = groups.map(_.deletes).sum
    val newRecords = inserts + updates + deletes
    val recordsWritten = groups.map(_.recordsWritten).sum
    val bytes = groups.map(_.bytesWritten).sum
    SegmentSummary(
      commits = commits,
      partitionsTouched = groups.map(_.partitionPath).distinct.size,
      fileGroupsTouched = groups.size,
      fileGroupsCreated = groups.count(_.createdInWindow),
      fileGroupsRewritten = groups.count(g => !g.createdInWindow),
      fileGroupsWithInserts = groups.count(_.hasInserts),
      fileGroupsWithUpdates = groups.count(_.hasUpdates),
      fileGroupsWithDeletes = groups.count(_.hasDeletes),
      inserts = inserts,
      updates = updates,
      deletes = deletes,
      recordsWritten = recordsWritten,
      bytesWritten = bytes,
      updateShareOfNewRecords = ratio(updates, newRecords),
      writeAmplification = ratio(recordsWritten, newRecords),
      bytesPerRecordWritten = ratio(bytes, recordsWritten),
      bytesPerNewRecord = ratio(bytes, newRecords),
      medianTouchesPerFileGroup = TimelineStats.median(groups.map(_.touches.toDouble)),
      medianAmplificationPerFileGroup = TimelineStats.median(groups.flatMap(_.amplification)))
  }

  private def ratio(num: Long, den: Long): Double =
    if (den == 0L) 0.0 else num.toDouble / den.toDouble

  /**
   * File groups that were written more than once as a base file, so their record
   * count at the start and end of the window are both known. This is the
   * "original vs current records in the file group" view.
   */
  def growthObservable(groups: Seq[FileGroupProfile]): List[FileGroupProfile] =
    groups.filter(_.baseFileTouches >= 2).toList

  /**
   * Wall-clock hours between two Hudi instant strings (`yyyyMMddHHmmssSSS`, or the
   * older `yyyyMMddHHmmss`). Reported alongside the commit count because most
   * figures here are rates over the window rather than intrinsic table
   * properties — a reader needs the span to interpret them. Returns None if
   * either instant doesn't parse.
   */
  def windowSpanHours(firstInstant: String, lastInstant: String): Option[Double] =
    for {
      a <- parseInstantMillis(firstInstant)
      b <- parseInstantMillis(lastInstant)
    } yield math.max(0.0, (b - a).toDouble / 3600000.0)

  private[utils] def parseInstantMillis(instant: String): Option[Long] = {
    val digits = instant.takeWhile(_.isDigit)
    if (digits.length < 14) return None
    try {
      val fmt = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
      fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
      val base = fmt.parse(digits.substring(0, 14)).getTime
      val millis = if (digits.length >= 17) digits.substring(14, 17).toLong else 0L
      Some(base + millis)
    } catch {
      case _: Exception => None
    }
  }

  /** Percentile of a double sequence using nearest-rank. Empty input returns 0.0. */
  def percentile(xs: Seq[Double], p: Double): Double = {
    if (xs.isEmpty) return 0.0
    val sorted = xs.sorted
    val idx =
      math.min(sorted.size - 1, math.max(0, math.round(p / 100.0 * (sorted.size - 1)).toInt))
    sorted(idx)
  }
}
