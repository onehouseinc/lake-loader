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

import ai.onehouse.lakeloader.configs.{SynthesizerConfig, WriteProfilerConfig}
import ai.onehouse.lakeloader.parser.WriteProfilerParser
import ai.onehouse.lakeloader.utils.WriteProfileStats
import ai.onehouse.lakeloader.utils.WriteProfileStats.{FileGroupProfile, SegmentSummary}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter

/**
 * Describes how an existing Hudi table is actually being written, rolled up
 * **per file group** rather than per partition.
 *
 * This answers a different question from WorkloadSynthesizer. The synthesizer
 * produces a ChangeDataGenerator config that reproduces a workload's shape; this
 * tool reports what the table did — how many file groups were created versus
 * rewritten, how many saw inserts/updates/deletes, how much each grew, and how
 * many records Hudi rewrote per record the writer contributed.
 *
 * Two things it does that the synthesizer deliberately does not:
 *
 *  1. **Separates table services from ingest.** Clustering and compaction are
 *     reported as their own population. Folding them into the workload can be
 *     wildly misleading — on a clustered table the rewrites routinely dwarf the
 *     ingest they accompany.
 *  2. **Reports write amplification.** `numWrites` is the record count of the
 *     file a write produced; `numInserts + numUpdates + numDeletes` is what the
 *     write contributed. Dividing bytes by the latter (as a naive record-size
 *     derivation does) overstates bytes/record by exactly the amplification
 *     factor.
 *
 * Read-only with respect to the source table: it reads the timeline and writes
 * only under --output-dir.
 */
object WriteProfiler {

  def main(args: Array[String]): Unit = {
    WriteProfilerParser.parser.parse(args, WriteProfilerConfig()) match {
      case Some(config) =>
        val spark = SparkSession.builder
          .appName("WriteProfilerApp")
          .getOrCreate()
        try {
          run(spark, config)
        } finally {
          spark.stop()
        }
      case None =>
        sys.exit(1)
    }
  }

  /** Everything the renderers need, in one bundle. */
  private[lakeloader] case class Profile(
      sourceTablePath: String,
      tableType: String,
      windowFirstInstant: String,
      windowLastInstant: String,
      totalCommits: Int,
      ingestCommits: Int,
      tableServiceCommits: Int,
      // Commits that actually carried write stats. An idle pipeline still emits
      // commits, so this can be a small fraction of totalCommits — in which case
      // every figure below rests on only those few commits.
      commitsWithWrites: Int,
      operationTypeCounts: List[(String, Int)],
      ingest: SegmentSummary,
      tableService: SegmentSummary,
      ingestGroups: List[FileGroupProfile],
      growth: List[FileGroupProfile],
      notes: Seq[String])

  private[lakeloader] def run(spark: SparkSession, config: WriteProfilerConfig): Unit = {
    require(config.tablePath.nonEmpty, "--table-path is required")
    require(config.outputDir.nonEmpty, "--output-dir is required")
    require(
      !normalize(config.outputDir).startsWith(normalize(config.tablePath)),
      s"--output-dir (${config.outputDir}) must not sit inside --table-path " +
        s"(${config.tablePath}); this tool must not write into the table it reads")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val storageConf = new HadoopStorageConfiguration(hadoopConf)
    val metaClient = HoodieTableMetaClient
      .builder()
      .setConf(storageConf)
      .setBasePath(config.tablePath)
      .build()

    // Reuse the synthesizer's timeline walk so both tools see identical inputs.
    val commits = WorkloadSynthesizer.loadCommits(
      metaClient,
      SynthesizerConfig(
        tablePath = config.tablePath,
        outputDir = config.outputDir,
        maxCommits = config.maxCommits,
        sinceInstant = config.sinceInstant))

    require(commits.nonEmpty, s"No completed commits found under ${config.tablePath}")

    val profile = derive(commits, metaClient.getTableType.name(), config.tablePath)
    writeOutputs(hadoopConf, config, profile)
    println(renderReport(profile, config.topFileGroups))
    println(
      s"[WriteProfiler] Wrote write-profile.txt, write-profile.json" +
        (if (config.emitFileGroupCsv) " and file-groups.csv" else "") +
        s" to ${config.outputDir}")
  }

  private def normalize(p: String): String = p.stripSuffix("/") + "/"

  private[lakeloader] def derive(
      commits: List[WorkloadSynthesizer.CommitAgg],
      tableType: String,
      sourceTablePath: String = ""): Profile = {
    val notes = scala.collection.mutable.ArrayBuffer[String]()

    val (serviceCommits, ingestCommits) = commits.partition(_.isTableService)

    val ingestWrites = ingestCommits.flatMap(_.fileGroupWrites).map(toWrite)
    val serviceWrites = serviceCommits.flatMap(_.fileGroupWrites).map(toWrite)

    val ingestGroups = WriteProfileStats.profileFileGroups(ingestWrites)
    val serviceGroups = WriteProfileStats.profileFileGroups(serviceWrites)

    val ingestSummary = WriteProfileStats.summarize(ingestCommits.size, ingestGroups)
    val serviceSummary = WriteProfileStats.summarize(serviceCommits.size, serviceGroups)

    // A window whose commits carry no write stats yields an all-zeros profile
    // that reads exactly like a real result. Refuse rather than mislead.
    val totalWrites = ingestSummary.recordsWritten + serviceSummary.recordsWritten
    require(
      totalWrites > 0L,
      s"All ${commits.size} commits in the analyzed window have empty write stats " +
        s"(0 records written). This usually means the window covers only empty " +
        s"batches and the table's real history has been archived. Widen the window " +
        s"with --since-instant, or lower the source table's archival threshold.")

    if (ingestSummary.recordsWritten == 0L) {
      notes += "no ingest writes in this window — every commit was a table service"
    }

    // An all-or-nothing guard is not enough: a window can pass it on a couple of
    // non-empty commits out of hundreds and still report confident-looking
    // figures. Observed on a real table with 642 ingest commits contributing 9
    // records total.
    val commitsWithWrites = commits.count(_.fileGroupWrites.exists(_.numWrites > 0L))
    val writeShare =
      if (commits.isEmpty) 0.0 else commitsWithWrites.toDouble / commits.size.toDouble
    if (writeShare < 0.25) {
      notes += f"ONLY $commitsWithWrites%d of ${commits.size}%d commits " +
        f"(${writeShare * 100}%.1f%%) carried any write stats — every figure here " +
        f"rests on those alone, and is unlikely to describe the table's steady " +
        f"state. The rest are empty batches from an idle pipeline. Treat this run " +
        f"as unrepresentative and widen the window with --since-instant."
    }
    if (serviceSummary.recordsWritten > ingestSummary.recordsWritten && serviceCommits.nonEmpty) {
      notes += f"table services wrote more records than ingest did " +
        f"(${serviceSummary.recordsWritten}%d vs ${ingestSummary.recordsWritten}%d) — " +
        f"a workload profile that counts both would be dominated by rewrites"
    }
    if (ingestSummary.writeAmplification > 2.0) {
      notes += "amplification and both bytes/record figures are rates over THIS " +
        "window, not intrinsic table properties: each rewrite of a file group " +
        "counts its whole record count again, so a longer window yields a larger " +
        "figure for the same table. Do not compare runs with different " +
        "--max-commits / --since-instant."
    }
    if (ingestSummary.writeAmplification > 2.0) {
      notes += f"ingest write amplification is ${ingestSummary.writeAmplification}%.2fx: " +
        f"Hudi wrote ${ingestSummary.recordsWritten}%d records to contribute " +
        f"${ingestSummary.inserts + ingestSummary.updates + ingestSummary.deletes}%d. " +
        f"bytes/record on a contributed-record basis " +
        f"(${ingestSummary.bytesPerNewRecord}%.1f) overstates the true figure " +
        f"(${ingestSummary.bytesPerRecordWritten}%.1f) by that factor."
    }
    if (ingestSummary.deletes == 0L) {
      notes += "no deletes observed in this window"
    }
    val logOnly = ingestGroups.count(_.baseFileTouches == 0)
    if (logOnly > 0) {
      notes += s"$logOnly of ${ingestGroups.size} ingest file groups saw only log " +
        s"appends; their record counts are per-block, so growth is not derivable for them"
    }

    val opCounts = commits
      .groupBy(_.operationType)
      .map { case (op, cs) => (op, cs.size) }
      .toList
      .sortBy { case (_, n) => -n }

    val instants = commits.map(_.instant).sorted

    Profile(
      sourceTablePath = sourceTablePath,
      tableType = tableType,
      windowFirstInstant = instants.head,
      windowLastInstant = instants.last,
      totalCommits = commits.size,
      ingestCommits = ingestCommits.size,
      tableServiceCommits = serviceCommits.size,
      commitsWithWrites = commitsWithWrites,
      operationTypeCounts = opCounts,
      ingest = ingestSummary,
      tableService = serviceSummary,
      ingestGroups = ingestGroups,
      growth = WriteProfileStats.growthObservable(ingestGroups),
      notes = notes.toSeq)
  }

  private def toWrite(w: WorkloadSynthesizer.FileGroupWrite): WriteProfileStats.Write =
    WriteProfileStats.Write(
      fileId = w.fileId,
      partitionPath = w.partitionPath,
      instant = w.instant,
      created = w.created,
      isBaseFile = w.isBaseFile,
      numWrites = w.numWrites,
      numInserts = w.numInserts,
      numUpdates = w.numUpdates,
      numDeletes = w.numDeletes,
      totalWriteBytes = w.totalWriteBytes,
      fileSizeInBytes = w.fileSizeInBytes)

  ///////////////////////
  // Rendering
  ///////////////////////

  private[lakeloader] def renderReport(p: Profile, topN: Int): String = {
    val sb = new StringBuilder
    sb.append("# WriteProfiler report\n")
    sb.append(s"source table: ${p.sourceTablePath}\n")
    sb.append(s"table type:   ${p.tableType}\n")
    sb.append(
      s"window:       ${p.windowFirstInstant} .. ${p.windowLastInstant} " +
        s"(${p.totalCommits} completed write commits" +
        WriteProfileStats
          .windowSpanHours(p.windowFirstInstant, p.windowLastInstant)
          .map(h => f", spanning ~$h%.1f h")
          .getOrElse("") + ")\n")
    sb.append(s"  ingest commits:        ${p.ingestCommits}\n")
    sb.append(s"  table-service commits: ${p.tableServiceCommits}\n")
    sb.append(s"  commits with writes:   ${p.commitsWithWrites} of ${p.totalCommits}\n")
    sb.append(
      s"  operation types:       " +
        p.operationTypeCounts.map { case (op, n) => s"$op=$n" }.mkString(", ") + "\n\n")

    sb.append(renderSegment("INGEST", p.ingest))
    sb.append("\n")
    sb.append(renderSegment("TABLE SERVICES (clustering / compaction)", p.tableService))

    if (p.growth.nonEmpty) {
      sb.append("\nfile-group growth (base-file writes only, top by records written):\n")
      sb.append(f"  ${"fileId"}%-40s ${"first"}%10s ${"last"}%10s ${"delta"}%10s\n")
      p.growth.take(topN).foreach { g =>
        val first = g.recordsAtFirstBaseWrite.getOrElse(0L)
        val last = g.recordsAtLastBaseWrite.getOrElse(0L)
        sb.append(f"  ${trunc(g.fileId, 40)}%-40s $first%10d $last%10d ${last - first}%+10d\n")
      }
      sb.append(
        s"  (${p.growth.size} of ${p.ingestGroups.size} ingest file groups had " +
          s">=2 base-file writes, so growth is observable for them)\n")
    }

    if (p.notes.nonEmpty) {
      sb.append("\nnotes:\n")
      p.notes.foreach(n => sb.append(s"  - $n\n"))
    }
    sb.toString
  }

  private def renderSegment(label: String, s: SegmentSummary): String = {
    val sb = new StringBuilder
    sb.append(s"## $label\n")
    sb.append(f"  commits                          ${s.commits}%d\n")
    sb.append(f"  partitions touched               ${s.partitionsTouched}%d\n")
    sb.append(f"  file groups touched              ${s.fileGroupsTouched}%d\n")
    sb.append(f"    created in window              ${s.fileGroupsCreated}%d\n")
    sb.append(f"    pre-existing (rewritten)       ${s.fileGroupsRewritten}%d\n")
    sb.append(f"    with inserts                   ${s.fileGroupsWithInserts}%d\n")
    sb.append(f"    with updates                   ${s.fileGroupsWithUpdates}%d\n")
    sb.append(f"    with deletes                   ${s.fileGroupsWithDeletes}%d\n")
    sb.append(f"  records contributed              ${s.inserts + s.updates + s.deletes}%d\n")
    sb.append(f"    inserts                        ${s.inserts}%d\n")
    sb.append(f"    updates                        ${s.updates}%d\n")
    sb.append(f"    deletes                        ${s.deletes}%d\n")
    sb.append(f"  records actually written         ${s.recordsWritten}%d\n")
    sb.append(f"  write amplification (window)     ${s.writeAmplification}%.2fx\n")
    sb.append(f"  update share of contributed      ${s.updateShareOfNewRecords * 100}%.1f%%\n")
    sb.append(f"  bytes written                    ${s.bytesWritten}%d\n")
    sb.append(f"  bytes/record (written basis)     ${s.bytesPerRecordWritten}%.1f\n")
    sb.append(f"  bytes/record (contributed basis) ${s.bytesPerNewRecord}%.1f\n")
    sb.append(f"  median touches per file group    ${s.medianTouchesPerFileGroup}%.1f\n")
    sb.append(f"  median amplification per fg      ${s.medianAmplificationPerFileGroup}%.2fx\n")
    sb.toString
  }

  private def trunc(s: String, n: Int): String = if (s.length <= n) s else s.take(n - 1) + "~"

  private[lakeloader] def renderJson(p: Profile): String = {
    def q(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    def seg(s: SegmentSummary): String =
      s"""{
         |      "commits": ${s.commits},
         |      "partitionsTouched": ${s.partitionsTouched},
         |      "fileGroupsTouched": ${s.fileGroupsTouched},
         |      "fileGroupsCreated": ${s.fileGroupsCreated},
         |      "fileGroupsRewritten": ${s.fileGroupsRewritten},
         |      "fileGroupsWithInserts": ${s.fileGroupsWithInserts},
         |      "fileGroupsWithUpdates": ${s.fileGroupsWithUpdates},
         |      "fileGroupsWithDeletes": ${s.fileGroupsWithDeletes},
         |      "inserts": ${s.inserts},
         |      "updates": ${s.updates},
         |      "deletes": ${s.deletes},
         |      "recordsWritten": ${s.recordsWritten},
         |      "bytesWritten": ${s.bytesWritten},
         |      "updateShareOfNewRecords": ${s.updateShareOfNewRecords},
         |      "writeAmplification": ${s.writeAmplification},
         |      "bytesPerRecordWritten": ${s.bytesPerRecordWritten},
         |      "bytesPerNewRecord": ${s.bytesPerNewRecord},
         |      "medianTouchesPerFileGroup": ${s.medianTouchesPerFileGroup},
         |      "medianAmplificationPerFileGroup": ${s.medianAmplificationPerFileGroup}
         |    }""".stripMargin

    s"""{
       |  "sourceTablePath": ${q(p.sourceTablePath)},
       |  "tableType": ${q(p.tableType)},
       |  "windowFirstInstant": ${q(p.windowFirstInstant)},
       |  "windowLastInstant": ${q(p.windowLastInstant)},
       |  "totalCommits": ${p.totalCommits},
       |  "ingestCommits": ${p.ingestCommits},
       |  "tableServiceCommits": ${p.tableServiceCommits},
       |  "commitsWithWrites": ${p.commitsWithWrites},
       |  "operationTypeCounts": {${p.operationTypeCounts
        .map { case (op, n) => s"${q(op)}: $n" }
        .mkString(", ")}},
       |  "ingest": ${seg(p.ingest)},
       |  "tableService": ${seg(p.tableService)},
       |  "fileGroupsWithObservableGrowth": ${p.growth.size},
       |  "notes": [${p.notes.map(q).mkString(", ")}]
       |}
       |""".stripMargin
  }

  private[lakeloader] def renderFileGroupCsv(p: Profile): String = {
    val sb = new StringBuilder
    sb.append(
      "fileId,partitionPath,touches,createdInWindow,baseFileTouches,firstInstant," +
        "lastInstant,recordsAtFirstBaseWrite,recordsAtLastBaseWrite,inserts,updates,deletes," +
        "recordsWritten,bytesWritten,amplification\n")
    p.ingestGroups.foreach { g =>
      sb.append(
        Seq(
          g.fileId,
          g.partitionPath,
          g.touches.toString,
          g.createdInWindow.toString,
          g.baseFileTouches.toString,
          g.firstInstant,
          g.lastInstant,
          g.recordsAtFirstBaseWrite.map(_.toString).getOrElse(""),
          g.recordsAtLastBaseWrite.map(_.toString).getOrElse(""),
          g.inserts.toString,
          g.updates.toString,
          g.deletes.toString,
          g.recordsWritten.toString,
          g.bytesWritten.toString,
          g.amplification.map(a => f"$a%.4f").getOrElse("")).mkString(","))
      sb.append("\n")
    }
    sb.toString
  }

  private def writeOutputs(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      config: WriteProfilerConfig,
      p: Profile): Unit = {
    val dir = new Path(config.outputDir)
    val fs = dir.getFileSystem(hadoopConf)
    if (!fs.exists(dir)) fs.mkdirs(dir)
    writeText(fs, new Path(dir, "write-profile.txt"), renderReport(p, config.topFileGroups))
    writeText(fs, new Path(dir, "write-profile.json"), renderJson(p))
    if (config.emitFileGroupCsv) {
      writeText(fs, new Path(dir, "file-groups.csv"), renderFileGroupCsv(p))
    }
  }

  private def writeText(fs: org.apache.hadoop.fs.FileSystem, path: Path, content: String): Unit = {
    var out: FSDataOutputStream = null
    try {
      out = fs.create(path, true)
      val pw = new PrintWriter(out)
      try pw.write(content)
      finally pw.flush()
    } finally {
      if (out != null) out.close()
    }
  }
}
