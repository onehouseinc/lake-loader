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

import ai.onehouse.lakeloader.utils.WriteProfileStats
import ai.onehouse.lakeloader.utils.WriteProfileStats.Write
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WriteProfileStatsSpec extends AnyFunSuite with Matchers {

  private def w(
      fileId: String,
      instant: String,
      numWrites: Long,
      inserts: Long = 0L,
      updates: Long = 0L,
      deletes: Long = 0L,
      created: Boolean = false,
      isBaseFile: Boolean = true,
      bytes: Long = 0L,
      partition: String = "p1"): Write =
    Write(
      fileId,
      partition,
      instant,
      created,
      isBaseFile,
      numWrites,
      inserts,
      updates,
      deletes,
      bytes,
      numWrites)

  test("rolls writes up by file group and orders history by instant") {
    val groups = WriteProfileStats.profileFileGroups(
      Seq(
        w("fg1", "003", 300, inserts = 30),
        w("fg1", "001", 100, inserts = 100, created = true),
        w("fg1", "002", 200, inserts = 20),
        w("fg2", "001", 50, inserts = 50, created = true)))

    groups.map(_.fileId) shouldBe List("fg1", "fg2")
    val fg1 = groups.head
    fg1.touches shouldBe 3
    fg1.firstInstant shouldBe "001"
    fg1.lastInstant shouldBe "003"
    fg1.createdInWindow shouldBe true
    fg1.recordsAtFirstBaseWrite shouldBe Some(100L)
    fg1.recordsAtLastBaseWrite shouldBe Some(300L)
    fg1.recordsWritten shouldBe 600L
    fg1.inserts shouldBe 150L
  }

  test("amplification is records written over records contributed") {
    // The real shape observed on a production table: 15722 records rewritten to
    // add 2378 new ones.
    val groups = WriteProfileStats.profileFileGroups(Seq(w("fg1", "001", 15722, inserts = 2378)))
    groups.head.amplification.get shouldBe (15722.0 / 2378.0 +- 1e-9)
  }

  test("amplification is undefined when a write contributed nothing") {
    val groups = WriteProfileStats.profileFileGroups(Seq(w("fg1", "001", 500)))
    groups.head.amplification shouldBe None
  }

  test("a file group that only saw log appends has no derivable growth") {
    val groups = WriteProfileStats.profileFileGroups(
      Seq(
        w("fg1", "001", 10, inserts = 10, isBaseFile = false),
        w("fg1", "002", 12, inserts = 12, isBaseFile = false)))
    groups.head.baseFileTouches shouldBe 0
    groups.head.recordsAtFirstBaseWrite shouldBe None
    WriteProfileStats.growthObservable(groups) shouldBe empty
  }

  test("growth is observable only with at least two base-file writes") {
    val one = WriteProfileStats.profileFileGroups(Seq(w("fg1", "001", 100, inserts = 100)))
    WriteProfileStats.growthObservable(one) shouldBe empty

    val two = WriteProfileStats.profileFileGroups(
      Seq(w("fg1", "001", 100, inserts = 100), w("fg1", "002", 180, inserts = 80)))
    WriteProfileStats.growthObservable(two).size shouldBe 1
  }

  test("summary counts created versus pre-existing file groups") {
    val groups = WriteProfileStats.profileFileGroups(
      Seq(
        w("fg1", "001", 100, inserts = 100, created = true),
        w("fg2", "001", 100, inserts = 100, created = true),
        w("fg3", "002", 500, updates = 20)))
    val s = WriteProfileStats.summarize(2, groups)
    s.fileGroupsTouched shouldBe 3
    s.fileGroupsCreated shouldBe 2
    s.fileGroupsRewritten shouldBe 1
  }

  test("summary counts file groups by the kind of change they received") {
    val groups = WriteProfileStats.profileFileGroups(
      Seq(
        w("fg1", "001", 100, inserts = 100),
        w("fg2", "001", 100, updates = 10),
        w("fg3", "001", 100, deletes = 5),
        w("fg4", "001", 100, inserts = 1, updates = 1, deletes = 1)))
    val s = WriteProfileStats.summarize(1, groups)
    s.fileGroupsWithInserts shouldBe 2
    s.fileGroupsWithUpdates shouldBe 2
    s.fileGroupsWithDeletes shouldBe 2
  }

  test("bytes/record differs between written and contributed bases by amplification") {
    val groups =
      WriteProfileStats.profileFileGroups(Seq(w("fg1", "001", 1000, inserts = 100, bytes = 72600)))
    val s = WriteProfileStats.summarize(1, groups)
    s.writeAmplification shouldBe (10.0 +- 1e-9)
    s.bytesPerRecordWritten shouldBe (72.6 +- 1e-9)
    s.bytesPerNewRecord shouldBe (726.0 +- 1e-9)
    // The contributed basis overstates by exactly the amplification factor.
    s.bytesPerNewRecord shouldBe (s.bytesPerRecordWritten * s.writeAmplification +- 1e-6)
  }

  test("update share is over contributed records, not written records") {
    val groups =
      WriteProfileStats.profileFileGroups(Seq(w("fg1", "001", 10000, inserts = 25, updates = 75)))
    val s = WriteProfileStats.summarize(1, groups)
    s.updateShareOfNewRecords shouldBe (0.75 +- 1e-9)
  }

  test("summary of an empty population is all zeros and does not divide by zero") {
    val s = WriteProfileStats.summarize(0, Nil)
    s.fileGroupsTouched shouldBe 0
    s.writeAmplification shouldBe 0.0
    s.bytesPerRecordWritten shouldBe 0.0
    s.updateShareOfNewRecords shouldBe 0.0
    s.medianAmplificationPerFileGroup shouldBe 0.0
  }

  test("partitions touched is distinct across file groups") {
    val groups = WriteProfileStats.profileFileGroups(
      Seq(
        w("fg1", "001", 10, inserts = 10, partition = "a"),
        w("fg2", "001", 10, inserts = 10, partition = "a"),
        w("fg3", "001", 10, inserts = 10, partition = "b")))
    WriteProfileStats.summarize(1, groups).partitionsTouched shouldBe 2
  }

  test("window span is derived from Hudi instant strings") {
    // 20260806173529301 .. 20260806213201949 -> ~3.94 h, the real 60-commit window.
    WriteProfileStats
      .windowSpanHours("20260806173529301", "20260806213201949")
      .get shouldBe (3.94 +- 0.02)
    // Second-precision instants (no millis suffix) parse too.
    WriteProfileStats.windowSpanHours("20260806120000", "20260806180000").get shouldBe
      (6.0 +- 1e-9)
    WriteProfileStats.windowSpanHours("bogus", "20260806180000") shouldBe None
  }

  test("amplification grows with window length for the same table") {
    // The reason writeAmplification is documented as a window-scoped rate: a file
    // group rewritten more times contributes its whole record count each time.
    def ampOver(rewrites: Int): Double = {
      val ws = (1 to rewrites).map { i =>
        w("fg1", f"$i%03d", numWrites = i * 1000L, inserts = 1000L)
      }
      WriteProfileStats.summarize(rewrites, WriteProfileStats.profileFileGroups(ws))
        .writeAmplification
    }
    val short = ampOver(5)
    val long = ampOver(50)
    long should be > short
  }

  test("percentile uses nearest rank and tolerates empty input") {
    WriteProfileStats.percentile(Nil, 50) shouldBe 0.0
    val xs = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    WriteProfileStats.percentile(xs, 0) shouldBe 1.0
    WriteProfileStats.percentile(xs, 50) shouldBe 3.0
    WriteProfileStats.percentile(xs, 100) shouldBe 5.0
  }

  test("table-service classification keys off operationType, compaction flag and action") {
    import WorkloadSynthesizer.isTableServiceCommit
    isTableServiceCommit(
      "CLUSTER",
      "replacecommit",
      compacted = false,
      tableIsMor = true) shouldBe true
    isTableServiceCommit("COMPACT", "commit", compacted = false, tableIsMor = true) shouldBe true
    isTableServiceCommit(
      "LOG_COMPACT",
      "deltacommit",
      compacted = false,
      tableIsMor = true) shouldBe true
    // Real production shape: ingest on a MoR table arrives as an INSERT deltacommit.
    isTableServiceCommit(
      "INSERT",
      "deltacommit",
      compacted = false,
      tableIsMor = true) shouldBe false
    isTableServiceCommit(
      "UPSERT",
      "deltacommit",
      compacted = false,
      tableIsMor = true) shouldBe false
    // A bare `commit` on MoR is compaction output even when operationType is unset.
    isTableServiceCommit("UNKNOWN", "commit", compacted = false, tableIsMor = true) shouldBe true
    // ...but on CoW it is ordinary ingest.
    isTableServiceCommit("UPSERT", "commit", compacted = false, tableIsMor = false) shouldBe false
    // The compacted flag alone is enough.
    isTableServiceCommit(
      "UNKNOWN",
      "deltacommit",
      compacted = true,
      tableIsMor = true) shouldBe true
  }
}
