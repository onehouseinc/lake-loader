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

import ai.onehouse.lakeloader.configs.KeyTypes.KeyType
import ai.onehouse.lakeloader.configs.{KeyTypes, SynthesizerConfig, UpdatePatterns}
import ai.onehouse.lakeloader.parser.WorkloadSynthesizerParser
import ai.onehouse.lakeloader.utils.TimelineStats
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieWriteStat}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayInputStream, PrintWriter}
import scala.collection.JavaConverters._

/**
 * Walks the active (optionally archived) timeline of an existing Hudi table and
 * emits a lake-loader ChangeDataGenerator configuration that reproduces the
 * observed workload characteristics. Two flag files are written side-by-side:
 * `synth-full.flags` (per-commit fidelity) and `synth-summary.flags` (single
 * median round). A companion `synth-audit.txt` records the raw derived numbers.
 */
object WorkloadSynthesizer {

  private val WRITE_ACTIONS: Set[String] = Set(
    HoodieTimeline.COMMIT_ACTION,
    HoodieTimeline.DELTA_COMMIT_ACTION,
    HoodieTimeline.REPLACE_COMMIT_ACTION)

  private val UUID_PREFIX = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-".r
  private val EPOCH_PREFIX = "^\\d{10,}".r

  /** Aggregated counts derived from a single commit's HoodieCommitMetadata. */
  private[lakeloader] case class CommitAgg(
      instant: String,
      action: String,
      inserts: Long,
      updates: Long,
      bytesWritten: Long,
      partitionInserts: Map[String, Long],
      partitionUpdates: Map[String, Long],
      freshFileSizes: Seq[Long])

  /** Everything the two flag emitters need, in one bundle. */
  private[lakeloader] case class DerivedConfig(
      numRounds: Int,
      recordsPerRound: List[Long],
      medianRecordsPerRound: Long,
      totalPartitions: Int,
      updateRatio: Double,
      numPartitionsToUpdate: Int,
      recordSize: Int,
      targetDataFileSize: Int,
      updatePattern: UpdatePatterns.UpdatePatterns,
      zipfShape: Double,
      partitionDistribution: List[Double],
      round0PartitionDistribution: Option[List[Double]],
      keyType: KeyType,
      keyTypeSource: String,
      recordKeyField: Option[String],
      auditNotes: Seq[String])

  def main(args: Array[String]): Unit = {
    WorkloadSynthesizerParser.parser.parse(args, SynthesizerConfig()) match {
      case Some(config) =>
        val spark = SparkSession.builder
          .appName("WorkloadSynthesizerApp")
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

  private[lakeloader] def run(spark: SparkSession, config: SynthesizerConfig): Unit = {
    require(config.tablePath.nonEmpty, "--table-path is required")
    require(config.outputDir.nonEmpty, "--output-dir is required")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val storageConf = new HadoopStorageConfiguration(hadoopConf)
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf)
      .setBasePath(config.tablePath)
      .build()

    val commits = loadCommits(metaClient, config)
    require(commits.nonEmpty, s"No completed commits found under ${config.tablePath}")

    val (keyType, keyTypeSource, recordKeyField, keyTypeNotes) =
      resolveKeyType(spark, metaClient, config)

    val derived = deriveConfig(commits, config, keyType, keyTypeSource, recordKeyField, keyTypeNotes)

    writeOutputs(hadoopConf, config.outputDir, derived, config.tablePath)
    println(s"[WorkloadSynthesizer] Wrote synth-full.flags, synth-summary.flags, and synth-audit.txt to ${config.outputDir}")
  }

  ///////////////////////
  // Timeline scanning
  ///////////////////////

  private def loadCommits(
      metaClient: HoodieTableMetaClient,
      config: SynthesizerConfig): List[CommitAgg] = {
    val timelines = if (config.includeArchived) {
      List(metaClient.getArchivedTimeline, metaClient.getActiveTimeline.getAllCommitsTimeline)
    } else {
      List(metaClient.getActiveTimeline.getAllCommitsTimeline)
    }
    val serde = metaClient.getTimelineLayout.getCommitMetadataSerDe

    val allInstants = timelines.flatMap(_.filterCompletedInstants().getInstants.iterator().asScala.toList)
      .filter(i => WRITE_ACTIONS.contains(i.getAction))
      .sortBy(_.requestedTime)

    val filtered = config.sinceInstant match {
      case Some(cut) => allInstants.filter(_.requestedTime.compareTo(cut) >= 0)
      case None => allInstants
    }

    val bounded = config.maxCommits match {
      case Some(n) if filtered.size > n => filtered.takeRight(n)
      case _ => filtered
    }

    bounded.flatMap { instant =>
      val details = metaClient.getActiveTimeline.getInstantDetails(instant)
      if (!details.isPresent) None
      else {
        val bytes = details.get()
        val metadata = deserializeCommitMetadata(serde, instant, bytes)
        Some(aggregateCommit(instant, metadata))
      }
    }
  }

  private def deserializeCommitMetadata(
      serde: org.apache.hudi.common.table.timeline.CommitMetadataSerDe,
      instant: HoodieInstant,
      bytes: Array[Byte]): HoodieCommitMetadata = {
    val in = new ByteArrayInputStream(bytes)
    val isEmpty: java.util.function.BooleanSupplier =
      new java.util.function.BooleanSupplier {
        override def getAsBoolean: Boolean = bytes.length == 0
      }
    try {
      serde.deserialize[HoodieCommitMetadata](
        instant,
        in,
        isEmpty,
        classOf[HoodieCommitMetadata])
    } finally {
      in.close()
    }
  }

  private def aggregateCommit(instant: HoodieInstant, metadata: HoodieCommitMetadata): CommitAgg = {
    var inserts = 0L
    var updates = 0L
    var bytesWritten = 0L
    val partitionInserts = scala.collection.mutable.HashMap[String, Long]()
    val partitionUpdates = scala.collection.mutable.HashMap[String, Long]()
    val freshFileSizes = scala.collection.mutable.ArrayBuffer[Long]()

    metadata.getPartitionToWriteStats.asScala.foreach { case (partition, stats) =>
      stats.asScala.foreach { s: HoodieWriteStat =>
        val ni = math.max(s.getNumInserts, 0L)
        val nu = math.max(s.getNumUpdateWrites, 0L)
        inserts += ni
        updates += nu
        bytesWritten += math.max(s.getTotalWriteBytes, 0L)
        if (ni > 0) partitionInserts(partition) = partitionInserts.getOrElse(partition, 0L) + ni
        if (nu > 0) partitionUpdates(partition) = partitionUpdates.getOrElse(partition, 0L) + nu
        // "null" (string) is Hudi's sentinel for "no previous file" — this write created a
        // brand-new base file. Its size is a clean signal for targetDataFileSize.
        val prev = s.getPrevCommit
        if ((prev == null || prev == "null") && s.getFileSizeInBytes > 0)
          freshFileSizes += s.getFileSizeInBytes
      }
    }

    CommitAgg(
      instant = instant.requestedTime,
      action = instant.getAction,
      inserts = inserts,
      updates = updates,
      bytesWritten = bytesWritten,
      partitionInserts = partitionInserts.toMap,
      partitionUpdates = partitionUpdates.toMap,
      freshFileSizes = freshFileSizes.toSeq)
  }

  ///////////////////////
  // Derivation
  ///////////////////////

  private[lakeloader] def deriveConfig(
      commits: List[CommitAgg],
      config: SynthesizerConfig,
      keyType: KeyType,
      keyTypeSource: String,
      recordKeyField: Option[String],
      keyTypeNotes: Seq[String]): DerivedConfig = {

    val recordsPerRound = commits.map(c => c.inserts + c.updates)
    val numRounds = commits.size
    val medianRecordsPerRound = TimelineStats.medianLong(recordsPerRound)

    val allPartitions = commits.flatMap(_.partitionInserts.keys).toSet ++
      commits.flatMap(_.partitionUpdates.keys).toSet
    val totalPartitions = allPartitions.size

    val updateRatio = TimelineStats.deriveUpdateRatio(commits.map(c => (c.inserts, c.updates)))

    // numPartitionsToUpdate: median across commits of "how many partitions saw an update"
    val partitionsUpdatedPerCommit = commits
      .filter(_.updates > 0)
      .map(_.partitionUpdates.size.toDouble)
    val numPartitionsToUpdate =
      if (partitionsUpdatedPerCommit.isEmpty) 0
      else math.max(1, TimelineStats.median(partitionsUpdatedPerCommit).round.toInt)

    // recordSize: compressed bytes/record over the whole horizon.
    val totalRecords = recordsPerRound.sum
    val totalBytes = commits.map(_.bytesWritten).sum
    val recordSize =
      if (totalRecords <= 0) 1024 else math.max(1, (totalBytes / totalRecords).toInt)

    // targetDataFileSize: median size of files that were freshly created (no prev commit).
    val allFresh = commits.flatMap(_.freshFileSizes)
    val targetDataFileSize =
      if (allFresh.isEmpty) 128 * 1024 * 1024
      else math.max(1024 * 1024, TimelineStats.medianLong(allFresh).toInt)

    // Zipf shape: fit per commit, take median. Prefer inserts if any commit had inserts;
    // otherwise use updates. Under min-threshold → Uniform.
    val insertShapes = commits.flatMap { c =>
      val vec = c.partitionInserts.values.toSeq.sorted(Ordering[Long].reverse)
      if (vec.size >= 2) Some(TimelineStats.fitZipfShape(vec)) else None
    }
    val updateShapes = commits.flatMap { c =>
      val vec = c.partitionUpdates.values.toSeq.sorted(Ordering[Long].reverse)
      if (vec.size >= 2) Some(TimelineStats.fitZipfShape(vec)) else None
    }
    val effectiveShapes = if (insertShapes.nonEmpty) insertShapes else updateShapes
    val fittedShape = if (effectiveShapes.isEmpty) 0.0 else TimelineStats.median(effectiveShapes)
    val (updatePattern, zipfShape) =
      if (fittedShape >= config.minZipfShapeToEmit)
        (UpdatePatterns.Zipf, roundTo(fittedShape, 3))
      else
        (UpdatePatterns.Uniform, 0.0)

    // Aggregate insert-share across commits, then compare round 0 vs the rest.
    val overallInsertShares = mergeInsertShares(commits)
    val partitionDistribution = TimelineStats.derivePartitionDistribution(overallInsertShares)

    val round0Distribution: Option[List[Double]] =
      if (commits.size >= 2) {
        val head = TimelineStats.derivePartitionDistribution(commits.head.partitionInserts)
        val tailAgg = mergeInsertShares(commits.tail)
        val tail = TimelineStats.derivePartitionDistribution(tailAgg)
        if (head.nonEmpty && tail.nonEmpty &&
            TimelineStats.distributionsDiffer(head, tail, eps = 0.05))
          Some(head)
        else None
      } else None

    val auditNotes = keyTypeNotes ++ Seq(
      s"commits considered: ${commits.size}",
      s"total records (inserts + updates): $totalRecords",
      s"total compressed bytes written: $totalBytes",
      s"partitions ever written: $totalPartitions",
      s"fitted zipf shapes (per commit, inserts): ${insertShapes.map(s => f"$s%.3f").mkString(", ")}",
      s"fitted zipf shapes (per commit, updates): ${updateShapes.map(s => f"$s%.3f").mkString(", ")}",
      s"round-0 differs from tail: ${round0Distribution.isDefined}")

    DerivedConfig(
      numRounds = numRounds,
      recordsPerRound = recordsPerRound,
      medianRecordsPerRound = medianRecordsPerRound,
      totalPartitions = totalPartitions,
      updateRatio = roundTo(updateRatio, 3),
      numPartitionsToUpdate = numPartitionsToUpdate,
      recordSize = recordSize,
      targetDataFileSize = targetDataFileSize,
      updatePattern = updatePattern,
      zipfShape = zipfShape,
      partitionDistribution = partitionDistribution.map(roundTo(_, 6)),
      round0PartitionDistribution = round0Distribution.map(_.map(roundTo(_, 6))),
      keyType = keyType,
      keyTypeSource = keyTypeSource,
      recordKeyField = recordKeyField,
      auditNotes = auditNotes)
  }

  private def mergeInsertShares(commits: Seq[CommitAgg]): Map[String, Long] = {
    val out = scala.collection.mutable.HashMap[String, Long]()
    commits.foreach { c =>
      c.partitionInserts.foreach { case (p, v) =>
        out(p) = out.getOrElse(p, 0L) + v
      }
    }
    out.toMap
  }

  private def roundTo(x: Double, decimals: Int): Double = {
    val f = math.pow(10, decimals)
    math.round(x * f) / f
  }

  ///////////////////////
  // Key-type inference
  ///////////////////////

  private def resolveKeyType(
      spark: SparkSession,
      metaClient: HoodieTableMetaClient,
      config: SynthesizerConfig): (KeyType, String, Option[String], Seq[String]) = {

    config.primaryKeyTypeOverride match {
      case Some(kt) => return (kt, "cli-override", None, Seq(s"key-type override supplied: $kt"))
      case None => // fall through
    }

    val tableConfig = metaClient.getTableConfig
    val recordKeyOpt = tableConfig.getRecordKeyFields // org.apache.hudi.common.util.Option[Array[String]]
    val keyFieldsOpt: Option[List[String]] =
      if (recordKeyOpt.isPresent) Some(recordKeyOpt.get().toList) else None
    val keyGen = Option(tableConfig.getKeyGeneratorClassName)
    val notes = scala.collection.mutable.ArrayBuffer[String](
      s"record key fields: ${keyFieldsOpt.map(_.mkString(",")).getOrElse("<unknown>")}",
      s"key generator class: ${keyGen.getOrElse("<unknown>")}")

    val keyFields = keyFieldsOpt.getOrElse(Nil)
    if (keyFields.size != 1) {
      notes += "composite or missing record key — emitting Random"
      return (KeyTypes.Random, "composite-or-missing-key", keyFieldsOpt.map(_.mkString(",")), notes.toSeq)
    }
    val keyCol = keyFields.head

    try {
      val samplePath = pickSampleParquetPath(metaClient)
      samplePath match {
        case None =>
          notes += "no base parquet file found to sample — defaulting to Random"
          (KeyTypes.Random, "no-sample-available", Some(keyCol), notes.toSeq)
        case Some(path) =>
          val samples = spark.read.parquet(path)
            .select(keyCol)
            .limit(config.keySampleSize)
            .collect()
            .flatMap(r => Option(r.get(0)).map(_.toString))
          notes += s"sampled ${samples.length} key values from $path"
          classifyKeySamples(samples, keyCol, notes)
      }
    } catch {
      case e: Exception =>
        notes += s"key sampling failed: ${e.getClass.getSimpleName}: ${e.getMessage}"
        (KeyTypes.Random, "sampling-failed", Some(keyCol), notes.toSeq)
    }
  }

  private def pickSampleParquetPath(metaClient: HoodieTableMetaClient): Option[String] = {
    val basePath = metaClient.getBasePath.toString
    val storage = metaClient.getStorage
    val root = new org.apache.hudi.storage.StoragePath(basePath)
    findFirstParquet(storage, root, maxDepth = 4)
  }

  private def findFirstParquet(
      storage: org.apache.hudi.storage.HoodieStorage,
      p: org.apache.hudi.storage.StoragePath,
      maxDepth: Int): Option[String] = {
    if (maxDepth < 0) return None
    val entriesTry =
      try Some(storage.listDirectEntries(p).asScala)
      catch { case _: Exception => None }
    entriesTry.flatMap { raw =>
      val visible = raw
        .filter(e => !e.getPath.getName.startsWith(".") && !e.getPath.getName.startsWith("_"))
      val hit = visible.collectFirst {
        case e if e.isFile && e.getPath.getName.endsWith(".parquet") => e.getPath.toString
      }
      hit.orElse {
        visible.iterator
          .filter(_.isDirectory)
          .flatMap(e => findFirstParquet(storage, e.getPath, maxDepth - 1).iterator)
          .toStream.headOption
      }
    }
  }

  private def classifyKeySamples(
      samples: Array[String],
      keyCol: String,
      notes: scala.collection.mutable.ArrayBuffer[String]): (KeyType, String, Option[String], Seq[String]) = {
    if (samples.isEmpty) {
      notes += "empty key sample — defaulting to Random"
      return (KeyTypes.Random, "empty-sample", Some(keyCol), notes.toSeq)
    }
    val uuidLike = samples.count(s => UUID_PREFIX.findFirstIn(s).isDefined)
    val epochLike = samples.count(s => EPOCH_PREFIX.findFirstIn(s).isDefined)
    val n = samples.length
    val monotonic = isNonDecreasing(samples)

    notes += f"key shape stats: uuid-prefix=${uuidLike.toDouble / n}%.2f, epoch-prefix=${epochLike.toDouble / n}%.2f, monotonic=$monotonic"

    if (uuidLike.toDouble / n > 0.9) {
      (KeyTypes.Random, "uuid-prefix-sample", Some(keyCol), notes.toSeq)
    } else if (epochLike.toDouble / n > 0.9 || monotonic) {
      (KeyTypes.TemporallyOrdered, "epoch-or-monotonic-sample", Some(keyCol), notes.toSeq)
    } else {
      notes += "sample looked ambiguous — defaulting to Random"
      (KeyTypes.Random, "ambiguous-sample-defaulted-random", Some(keyCol), notes.toSeq)
    }
  }

  private def isNonDecreasing(samples: Array[String]): Boolean = {
    if (samples.length < 2) return false
    samples.sliding(2).forall(pair => pair(0).compareTo(pair(1)) <= 0)
  }

  ///////////////////////
  // Output writers
  ///////////////////////

  private def writeOutputs(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      outputDir: String,
      d: DerivedConfig,
      sourceTablePath: String): Unit = {
    val dir = new Path(outputDir)
    val fs = dir.getFileSystem(hadoopConf)
    if (!fs.exists(dir)) fs.mkdirs(dir)

    writeText(fs, new Path(dir, "synth-full.flags"), renderFullFlags(d))
    writeText(fs, new Path(dir, "synth-summary.flags"), renderSummaryFlags(d))
    writeText(fs, new Path(dir, "synth-audit.txt"), renderAudit(d, sourceTablePath))
  }

  private def writeText(fs: org.apache.hadoop.fs.FileSystem, path: Path, content: String): Unit = {
    var out: FSDataOutputStream = null
    try {
      out = fs.create(path, true)
      val pw = new PrintWriter(out)
      try pw.write(content) finally pw.flush()
    } finally {
      if (out != null) out.close()
    }
  }

  private[lakeloader] def renderFullFlags(d: DerivedConfig): String = {
    val recordsPerRound = d.recordsPerRound.mkString(",")
    val lines = commonFlagLines(d) ++ Seq(
      s"--number-rounds ${d.numRounds}",
      s"--number-records-per-round $recordsPerRound")
    lines.mkString("\n") + "\n"
  }

  private[lakeloader] def renderSummaryFlags(d: DerivedConfig): String = {
    val lines = commonFlagLines(d) ++ Seq(
      s"--number-rounds ${d.numRounds}",
      s"--number-records-per-round ${d.medianRecordsPerRound}")
    lines.mkString("\n") + "\n"
  }

  private def commonFlagLines(d: DerivedConfig): Seq[String] = {
    val partDist = d.round0PartitionDistribution match {
      case Some(head) =>
        s"--partition-distribution '${head.mkString(",")};${d.partitionDistribution.mkString(",")}'"
      case None =>
        if (d.partitionDistribution.isEmpty) ""
        else s"--partition-distribution '${d.partitionDistribution.mkString(",")}'"
    }

    val base = Seq(
      "--path <fill-in>",
      "--avro-schema <fill-in>.avsc",
      s"--total-partitions ${d.totalPartitions}",
      s"--record-size ${d.recordSize}",
      s"--datagen-file-size ${d.targetDataFileSize}",
      s"--update-ratio ${d.updateRatio}",
      s"--num-partitions-to-update ${d.numPartitionsToUpdate}",
      s"--update-pattern ${d.updatePattern}",
      s"--primary-key-type ${d.keyType}")

    val withZipf =
      if (d.updatePattern == UpdatePatterns.Zipf) base :+ s"--zipfian-shape ${d.zipfShape}"
      else base
    if (partDist.isEmpty) withZipf else withZipf :+ partDist
  }

  private[lakeloader] def renderAudit(d: DerivedConfig, sourceTablePath: String): String = {
    val header = Seq(
      "# WorkloadSynthesizer audit",
      s"source table: $sourceTablePath",
      s"key type source: ${d.keyTypeSource}",
      s"record key field: ${d.recordKeyField.getOrElse("<unknown>")}",
      "",
      "derived-values:",
      s"  numRounds=${d.numRounds}",
      s"  totalPartitions=${d.totalPartitions}",
      s"  updateRatio=${d.updateRatio}",
      s"  numPartitionsToUpdate=${d.numPartitionsToUpdate}",
      s"  recordSize=${d.recordSize}",
      s"  targetDataFileSize=${d.targetDataFileSize}",
      s"  updatePattern=${d.updatePattern}",
      s"  zipfShape=${d.zipfShape}",
      s"  keyType=${d.keyType}",
      s"  medianRecordsPerRound=${d.medianRecordsPerRound}",
      "",
      "audit-notes:")
    val notes = d.auditNotes.map(n => s"  - $n")
    val partsHead = Seq(
      "",
      "partition-distribution-leading-weights (up to 20):",
      s"  ${d.partitionDistribution.take(20).mkString(",")}")
    val round0 = d.round0PartitionDistribution match {
      case Some(w) => Seq(
        "",
        "round-0-partition-distribution-leading-weights (up to 20):",
        s"  ${w.take(20).mkString(",")}")
      case None => Nil
    }
    val recordsPerRoundPreview = if (d.recordsPerRound.size <= 20) d.recordsPerRound.mkString(",")
    else d.recordsPerRound.take(20).mkString(",") + s",... (${d.recordsPerRound.size - 20} more)"
    val roundsPreview = Seq(
      "",
      "records-per-round (preview):",
      s"  $recordsPerRoundPreview")

    (header ++ notes ++ partsHead ++ round0 ++ roundsPreview).mkString("\n") + "\n"
  }
}
