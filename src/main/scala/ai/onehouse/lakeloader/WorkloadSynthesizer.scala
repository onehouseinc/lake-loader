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
import ai.onehouse.lakeloader.utils.{AvroSchemaUtils, TimelineStats}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieWriteStat}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayInputStream, PrintWriter}
import scala.collection.JavaConverters._

/**
 * Walks the active timeline of an existing Hudi table and emits a lake-loader
 * ChangeDataGenerator configuration that reproduces the observed workload
 * characteristics. Two flag files are written side-by-side: `synth-full.flags`
 * (per-commit fidelity) and `synth-summary.flags` (single median round). A
 * companion `synth-audit.txt` records the raw derived numbers.
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
      freshFileSizes: Seq[Long],
      // Absolute paths of base parquet files this commit wrote. Used by the
      // key-type resolver to sample footer stats from the most recent commits
      // rather than doing a full-table directory walk. Empty for commits that
      // only appended log files (MoR delta-commits with no new base files).
      writtenParquetPaths: Seq[String] = Seq.empty)

  /**
   * How the emitted flag file should describe the schema shape.
   *
   *  - `SuppliedSchema(path)`: customer handed off an .avsc; emit --avro-schema
   *    and drop --number-columns.
   *  - `InferredColumnCount(n)`: no schema; emit --number-columns matching the
   *    source Hudi table's top-level field arity and drop --avro-schema.
   */
  sealed trait SchemaChoice
  final case class SuppliedSchema(path: String) extends SchemaChoice
  final case class InferredColumnCount(numColumns: Int) extends SchemaChoice

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
      schemaChoice: SchemaChoice,
      // Record-Level Index mode observed on the source table's metadata.
      // One of "none" (RLI not enabled), "global" (single flat file-group set
      // covering all keys), "partitioned" (one file-group set per data
      // partition, encoded into the RLI file-IDs), or "unknown" (RLI is
      // enabled but the file-id shape didn't match either known pattern —
      // see audit for a sample).
      rliMode: String = "none",
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
      resolveKeyType(spark, metaClient, config, commits)

    val (schemaChoice, schemaNotes) = resolveSchemaChoice(metaClient, config)

    val (rliMode, rliNotes) = resolveRliMode(metaClient)

    val derived = deriveConfig(
      commits, config, keyType, keyTypeSource, recordKeyField, schemaChoice,
      auditNotesPrefix = keyTypeNotes ++ schemaNotes ++ rliNotes,
      rliMode = rliMode)

    writeOutputs(hadoopConf, config.outputDir, derived, config.tablePath)
    println(s"[WorkloadSynthesizer] Wrote synth-full.flags, synth-summary.flags, and synth-audit.txt to ${config.outputDir}")
  }

  ///////////////////////
  // Timeline scanning
  ///////////////////////

  private def loadCommits(
      metaClient: HoodieTableMetaClient,
      config: SynthesizerConfig): List[CommitAgg] = {
    // Only the active timeline is walked. Archived timeline is intentionally
    // excluded — HoodieArchivedTimeline needs its own instant-details reader,
    // and "recent workload characterization" (last N commits in active) is the
    // useful lens for benchmarking. Users wanting more history can dial
    // Hudi's archival threshold on the source table.
    val activeTimeline = metaClient.getActiveTimeline.getAllCommitsTimeline
    val serde = metaClient.getTimelineLayout.getCommitMetadataSerDe

    val allInstants = activeTimeline.filterCompletedInstants().getInstants.iterator().asScala.toList
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

    val basePath = metaClient.getBasePath
    bounded.flatMap { instant =>
      val details = metaClient.getActiveTimeline.getInstantDetails(instant)
      if (!details.isPresent) None
      else {
        val bytes = details.get()
        val metadata = deserializeCommitMetadata(serde, instant, bytes)
        Some(aggregateCommit(instant, metadata, basePath))
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

  private def aggregateCommit(
      instant: HoodieInstant,
      metadata: HoodieCommitMetadata,
      basePath: org.apache.hudi.storage.StoragePath): CommitAgg = {
    var inserts = 0L
    var updates = 0L
    var bytesWritten = 0L
    val partitionInserts = scala.collection.mutable.HashMap[String, Long]()
    val partitionUpdates = scala.collection.mutable.HashMap[String, Long]()
    val freshFileSizes = scala.collection.mutable.ArrayBuffer[Long]()
    val parquetPaths = scala.collection.mutable.ArrayBuffer[String]()

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
        // Capture the full base-parquet path written by this stat. Skip log
        // files (MoR delta commits) — key-type inference only reads parquet.
        val rel = s.getPath
        if (rel != null && rel.endsWith(".parquet")) {
          parquetPaths += new org.apache.hudi.storage.StoragePath(basePath, rel).toString
        }
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
      freshFileSizes = freshFileSizes.toSeq,
      writtenParquetPaths = parquetPaths.toSeq)
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
      schemaChoice: SchemaChoice,
      auditNotesPrefix: Seq[String] = Nil,
      rliMode: String = "none"): DerivedConfig = {

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
    // updateShapes is a defensive fallback for tables where every commit was
    // pure updates (no inserts on any commit — rare, e.g. long-lived
    // compaction-only or restore-heavy tables). Normal tables always have
    // inserts in round 0 so insertShapes drives the fit.
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

    val auditNotes = auditNotesPrefix ++ Seq(
      s"commits considered: ${commits.size}",
      s"total records (inserts + updates): $totalRecords",
      s"total compressed bytes written: $totalBytes",
      s"partitions ever written: $totalPartitions",
      s"fitted zipf shapes (per commit, inserts): ${insertShapes.map(s => f"$s%.3f").mkString(", ")}",
      s"fitted zipf shapes (per commit, updates): ${updateShapes.map(s => f"$s%.3f").mkString(", ")}",
      f"median fitted zipf shape=$fittedShape%.3f, min-zipf-threshold=${config.minZipfShapeToEmit}%.3f -> $updatePattern",
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
      schemaChoice = schemaChoice,
      rliMode = rliMode,
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
  // RLI mode detection
  ///////////////////////

  private val RLI_METADATA_PARTITION = "record_index"
  private val RLI_FILE_ID_PREFIX = "record-index-"
  // Global RLI file-IDs: full ID is `record-index-<0000>-<0>`. After stripping
  // the "record-index-" prefix, what's left matches `\d{4,}-\d+`.
  private val RLI_GLOBAL_SUFFIX = "^\\d{4,}-\\d+$".r
  // Partitioned RLI file-IDs: full ID is `record-index-<encoded-partition>-<0000>-<0>`.
  // After stripping the prefix, what's left ends with `-\d{4,}-\d+` and has
  // a non-empty partition segment before that.
  private val RLI_PARTITIONED_SUFFIX = ".+-\\d{4,}-\\d+$".r

  /**
   * Detect whether the source table has RLI enabled, and if so whether it's
   * global or partitioned. Classification is by file-ID naming in the
   * `record_index` metadata partition:
   *
   *  - Global RLI: file-IDs look like `record-index-0000-0`, `record-index-0001-0`.
   *  - Partitioned RLI: file-IDs look like `record-index-<encoded-partition>-0000-0`.
   *
   * The trailing `-\d{4,}-\d+` is stripped; if what's left is exactly the
   * literal `record-index-`, it's global. If there's a residual segment
   * between the prefix and the numeric tail, it's a partition path.
   *
   * Defensive: unrecognized shapes return "unknown" with an audit note; the
   * synthesizer run does not fail.
   */
  private[lakeloader] def resolveRliMode(
      metaClient: HoodieTableMetaClient): (String, Seq[String]) = {
    val notes = scala.collection.mutable.ArrayBuffer[String]()
    val enabledPartitions =
      try metaClient.getTableConfig.getMetadataPartitions.asScala
      catch {
        case e: Exception =>
          notes += s"RLI detection: could not read metadata partitions: ${e.getClass.getSimpleName}: ${e.getMessage}"
          return ("none", notes.toSeq)
      }
    if (!enabledPartitions.contains(RLI_METADATA_PARTITION)) {
      notes += "RLI: not enabled on source table (record_index absent from metadata partitions)"
      return ("none", notes.toSeq)
    }

    val storage = metaClient.getStorage
    val rliDir = new org.apache.hudi.storage.StoragePath(
      metaClient.getBasePath.toString + "/.hoodie/metadata/" + RLI_METADATA_PARTITION)

    val fileIds: List[String] =
      try {
        storage.listDirectEntries(rliDir).asScala
          .filter(e => e.isFile)
          .map(_.getPath.getName)
          .filter(name => !name.startsWith(".") && !name.startsWith("_"))
          // Hudi base file names: <fileId>_<writeToken>_<instantTime>.<ext>
          .map(name => name.split("_").headOption.getOrElse(""))
          .filter(_.startsWith(RLI_FILE_ID_PREFIX))
          .toList.distinct
      } catch {
        case e: Exception =>
          notes += s"RLI: could not list metadata dir $rliDir: ${e.getClass.getSimpleName}: ${e.getMessage}"
          return ("unknown", notes.toSeq)
      }

    if (fileIds.isEmpty) {
      notes += s"RLI: metadata partition $RLI_METADATA_PARTITION exists but no file-IDs found; treating as unknown"
      return ("unknown", notes.toSeq)
    }

    val (mode, distinct) = classifyRliFileIds(fileIds)
    notes += s"RLI: sampled ${fileIds.size} file-IDs from $RLI_METADATA_PARTITION; classifications=${distinct.mkString(",")}"
    notes += s"RLI: sample file-IDs: ${fileIds.take(3).mkString(", ")}"
    if (mode == "unknown") {
      notes += s"RLI: mixed or unrecognized file-ID shapes ($distinct); emitting 'unknown'"
    }
    (mode, notes.toSeq)
  }

  /**
   * Pure classification helper. Given a list of RLI file-IDs, decide whether
   * they represent a global RLI, a partitioned RLI, or an unrecognized shape.
   * Returns (mode, distinctPerIdClassifications) so callers can log the raw
   * per-ID breakdown when the aggregate is "unknown".
   */
  private[lakeloader] def classifyRliFileIds(fileIds: Seq[String]): (String, Set[String]) = {
    if (fileIds.isEmpty) return ("unknown", Set.empty)
    val classifications = fileIds.map { fid =>
      if (!fid.startsWith(RLI_FILE_ID_PREFIX)) "unknown"
      else {
        val stripped = fid.stripPrefix(RLI_FILE_ID_PREFIX)
        if (RLI_GLOBAL_SUFFIX.pattern.matcher(stripped).matches()) "global"
        else if (RLI_PARTITIONED_SUFFIX.pattern.matcher(stripped).matches()) "partitioned"
        else "unknown"
      }
    }.toSet
    val mode =
      if (classifications == Set("global")) "global"
      else if (classifications == Set("partitioned")) "partitioned"
      else "unknown"
    (mode, classifications)
  }

  ///////////////////////
  // Schema resolution
  ///////////////////////

  /**
   * Decide whether the emitted flag file should reference an .avsc (supplied
   * or written from the source table) or fall back to --number-columns. When
   * we need to write an .avsc (either anonymized, or copied from the customer
   * for reference), the file is dropped into outputDir alongside the flag
   * files as `schema.avsc`.
   */
  private[lakeloader] def resolveSchemaChoice(
      metaClient: HoodieTableMetaClient,
      config: SynthesizerConfig): (SchemaChoice, Seq[String]) = {
    val notes = scala.collection.mutable.ArrayBuffer[String]()
    val hadoopConf = metaClient.getStorageConf.unwrapAs(classOf[org.apache.hadoop.conf.Configuration])
    val outSchemaPath = new Path(config.outputDir, "schema.avsc")

    (config.schemaFile, config.anonymizeSchema) match {
      case (Some(path), false) =>
        notes += s"schema supplied by user: $path (no anonymization)"
        (SuppliedSchema(path), notes.toSeq)

      case (Some(path), true) =>
        val original = AvroSchemaUtils.parseAvroSchemaFile(path, hadoopConf)
        val anonymized = anonymizeAvroSchema(original)
        writeAvroSchema(hadoopConf, outSchemaPath, anonymized)
        notes += s"schema supplied by user: $path (anonymized to ${outSchemaPath.toString})"
        (SuppliedSchema(outSchemaPath.toString), notes.toSeq)

      case (None, true) =>
        val original = readSourceTableAvroSchema(metaClient)
        original match {
          case Some(schema) =>
            val anonymized = anonymizeAvroSchema(schema)
            writeAvroSchema(hadoopConf, outSchemaPath, anonymized)
            notes += s"schema inferred from source table, anonymized to ${outSchemaPath.toString}"
            (SuppliedSchema(outSchemaPath.toString), notes.toSeq)
          case None =>
            notes += "no source table schema available; falling back to --number-columns"
            val n = countSourceTableColumns(metaClient).getOrElse(10)
            (InferredColumnCount(n), notes.toSeq)
        }

      case (None, false) =>
        val n = countSourceTableColumns(metaClient).getOrElse(10)
        notes += s"no schema supplied; emitting --number-columns=$n (top-level field count from source Hudi table)"
        (InferredColumnCount(n), notes.toSeq)
    }
  }

  private def readSourceTableAvroSchema(metaClient: HoodieTableMetaClient): Option[org.apache.avro.Schema] = {
    val opt = metaClient.getTableConfig.getTableCreateSchema
    if (opt.isPresent) Some(opt.get()) else None
  }

  private def countSourceTableColumns(metaClient: HoodieTableMetaClient): Option[Int] = {
    readSourceTableAvroSchema(metaClient).map(_.getFields.size())
  }

  /**
   * Rewrite the top-level RECORD's field names to typed placeholders like
   * col_int_a, col_long_b, col_string_c. Preserves data types, nullability,
   * and default values. Only top-level names are rewritten — nested record
   * field names are also anonymized recursively; enums, arrays, maps
   * likewise carry their inner types through unchanged.
   */
  private[lakeloader] def anonymizeAvroSchema(schema: org.apache.avro.Schema): org.apache.avro.Schema = {
    import org.apache.avro.Schema
    def suffix(idx: Int): String = {
      // 0→a, 1→b, 25→z, 26→aa, 27→ab, ...
      val sb = new StringBuilder
      var n = idx
      do {
        sb.append(('a' + (n % 26)).toChar)
        n = n / 26 - 1
      } while (n >= 0)
      sb.reverse.toString
    }
    def typeTag(s: Schema): String = s.getType match {
      case Schema.Type.INT => "int"
      case Schema.Type.LONG => "long"
      case Schema.Type.FLOAT => "float"
      case Schema.Type.DOUBLE => "double"
      case Schema.Type.STRING => "string"
      case Schema.Type.BOOLEAN => "bool"
      case Schema.Type.BYTES => "bytes"
      case Schema.Type.FIXED => "fixed"
      case Schema.Type.RECORD => "record"
      case Schema.Type.ARRAY => "array"
      case Schema.Type.MAP => "map"
      case Schema.Type.ENUM => "enum"
      case Schema.Type.UNION =>
        // Strip the trailing NULL branch (nullable) and tag by the remaining type.
        val nonNull = s.getTypes.asScala.filter(_.getType != Schema.Type.NULL)
        if (nonNull.size == 1) typeTag(nonNull.head) else "union"
      case _ => "other"
    }

    if (schema.getType != Schema.Type.RECORD) return schema
    val newRecord = Schema.createRecord(
      "SynthAnonRecord",
      null,
      "ai.onehouse.lakeloader.synth",
      false)
    val newFields = new java.util.ArrayList[Schema.Field]()
    schema.getFields.asScala.zipWithIndex.foreach { case (f, i) =>
      val anonName = s"col_${typeTag(f.schema())}_${suffix(i)}"
      val innerSchema = f.schema().getType match {
        case Schema.Type.RECORD => anonymizeAvroSchema(f.schema())
        case _ => f.schema()
      }
      newFields.add(new Schema.Field(anonName, innerSchema, null, f.defaultVal()))
    }
    newRecord.setFields(newFields)
    newRecord
  }

  private def writeAvroSchema(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      path: Path,
      schema: org.apache.avro.Schema): Unit = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) fs.mkdirs(path.getParent)
    var out: FSDataOutputStream = null
    try {
      out = fs.create(path, true)
      val pw = new PrintWriter(out)
      try pw.write(schema.toString(true)) finally pw.flush()
    } finally {
      if (out != null) out.close()
    }
  }

  ///////////////////////
  // Key-type inference
  ///////////////////////

  private def resolveKeyType(
      spark: SparkSession,
      metaClient: HoodieTableMetaClient,
      config: SynthesizerConfig,
      commits: List[CommitAgg]): (KeyType, String, Option[String], Seq[String]) = {

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

    // Sample parquet paths from the last N completed commits (deterministic
    // and cheap — no full-table directory walk).
    val recentCommits = commits.takeRight(math.max(1, config.keySampleCommits))
    val candidatePaths = recentCommits.flatMap(_.writtenParquetPaths).take(config.keySampleFiles)
    notes += s"key-type sample source: last ${recentCommits.size} completed commits, ${candidatePaths.size} candidate parquet files"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val footerFailures = scala.collection.mutable.ArrayBuffer.empty[String]
    val sampled: List[FooterSample] = candidatePaths.flatMap { pathStr =>
      readFooterKeyStats(pathStr, keyCol, hadoopConf) match {
        case Right((minOpt, maxOpt)) =>
          Some(FooterSample(
            path = pathStr,
            instantTime = extractInstantFromFileName(pathStr),
            min = minOpt,
            max = maxOpt))
        case Left(reason) =>
          footerFailures += s"$pathStr: $reason"
          None
      }
    }
    notes += s"sampled ${sampled.size} base parquet file footers for record-key column '$keyCol'"
    if (footerFailures.nonEmpty) {
      notes += s"footer read failures: ${footerFailures.size} (showing up to 3)"
      footerFailures.take(3).foreach(f => notes += s"  - $f")
    }

    if (sampled.size >= 3) {
      val (kt, source, extra) = classifyFromFooterStats(sampled)
      notes ++= extra
      (kt, source, Some(keyCol), notes.toSeq)
    } else {
      // Not enough files for a footer-based inference. Fall back to reading actual
      // key values from whatever single file we did find.
      notes += "fewer than 3 base files available for footer sampling — falling back to value read"
      sampled.headOption match {
        case None =>
          notes += "no base parquet file found — defaulting to Random"
          (KeyTypes.Random, "no-sample-available", Some(keyCol), notes.toSeq)
        case Some(fs) =>
          try {
            val samples = spark.read.parquet(fs.path)
              .select(keyCol)
              .limit(config.keySampleSize)
              .collect()
              .flatMap(r => Option(r.get(0)).map(_.toString))
            notes += s"sampled ${samples.length} key values from ${fs.path}"
            classifyKeyValueSamples(samples, keyCol, notes)
          } catch {
            case e: Exception =>
              notes += s"value read failed: ${e.getClass.getSimpleName}: ${e.getMessage}"
              (KeyTypes.Random, "sampling-failed", Some(keyCol), notes.toSeq)
          }
      }
    }
  }

  ///////////////////////
  // Footer-based key sampling
  ///////////////////////

  private[lakeloader] case class FooterSample(
      path: String,
      instantTime: String,
      min: Option[String],
      max: Option[String])

  /**
   * Hudi encodes base file names as `<fileId>_<writeToken>_<instantTime>.parquet`.
   * We only need the instant string; empty if the pattern doesn't match.
   */
  private[lakeloader] def extractInstantFromFileName(pathStr: String): String = {
    val name = pathStr.substring(pathStr.lastIndexOf('/') + 1)
    val stem = if (name.endsWith(".parquet")) name.dropRight(".parquet".length) else name
    val parts = stem.split("_")
    if (parts.length >= 3) parts.last else ""
  }

  /**
   * Read the parquet footer of `path` and locate `keyCol`; aggregate min/max across
   * all row groups. Returns `Right((min, max))` on success (either bound may be None
   * if the file had no non-null statistics for the column), or `Left(reason)` when
   * the footer couldn't be read at all. Callers accumulate the failure reasons and
   * surface them in the audit rather than silently dropping files.
   */
  private def readFooterKeyStats(
      path: String,
      keyCol: String,
      hadoopConf: org.apache.hadoop.conf.Configuration): Either[String, (Option[String], Option[String])] = {
    import org.apache.parquet.hadoop.ParquetFileReader
    import org.apache.parquet.format.converter.ParquetMetadataConverter
    try {
      val meta = ParquetFileReader.readFooter(
        hadoopConf,
        new org.apache.hadoop.fs.Path(path),
        ParquetMetadataConverter.NO_FILTER)
      var overallMin: Option[String] = None
      var overallMax: Option[String] = None
      meta.getBlocks.asScala.foreach { block =>
        block.getColumns.asScala.foreach { col =>
          if (col.getPath.toDotString == keyCol || col.getPath.toArray.lastOption.contains(keyCol)) {
            val stats = col.getStatistics
            if (stats != null && !stats.isEmpty && stats.hasNonNullValue) {
              val mn = Option(stats.genericGetMin).map(_.toString)
              val mx = Option(stats.genericGetMax).map(_.toString)
              overallMin = combineMin(overallMin, mn)
              overallMax = combineMax(overallMax, mx)
            }
          }
        }
      }
      Right((overallMin, overallMax))
    } catch {
      case e: Exception => Left(s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }

  private def combineMin(a: Option[String], b: Option[String]): Option[String] = (a, b) match {
    case (None, x) => x
    case (x, None) => x
    case (Some(x), Some(y)) => Some(if (x.compareTo(y) <= 0) x else y)
  }

  private def combineMax(a: Option[String], b: Option[String]): Option[String] = (a, b) match {
    case (None, x) => x
    case (x, None) => x
    case (Some(x), Some(y)) => Some(if (x.compareTo(y) >= 0) x else y)
  }

  /**
   * Classification from footer min/max samples ordered by commit instant time.
   * Three signals, in order of decreasing confidence:
   *
   *  - **Random / UUID-shaped**: min values start with the low hex domain
   *    (mostly '0'..'3') and max values start with the high hex domain
   *    (mostly 'c'..'f') in almost every sampled file. Each file individually
   *    spans a wide chunk of the [0..f] hex domain.
   *  - **TemporallyOrdered**: file min/max monotonically increase with instant
   *    time (Spearman rank correlation ≥ 0.7 between instant and min).
   *  - **Hybrid**: temporal correlation is strong (≥ 0.7) but per-file range
   *    is still wide (min ≠ max prefix). Treated as TemporallyOrdered downstream
   *    with a note in the audit.
   */
  private[lakeloader] def classifyFromFooterStats(
      samples: List[FooterSample]): (KeyType, String, Seq[String]) = {
    val notes = scala.collection.mutable.ArrayBuffer[String]()
    val valid = samples.filter(s => s.min.isDefined && s.max.isDefined)
    if (valid.size < 3) {
      notes += s"only ${valid.size} sampled files had usable min/max stats — defaulting to Random"
      return (KeyTypes.Random, "insufficient-footer-stats", notes.toSeq)
    }

    // Signal 1: UUID-domain saturation. UUIDs are lowercase hex; a random-hash
    // key column will have mins near '0' and maxes near 'f' in most files.
    // We also handle prefix-namespaced keys like "<tenant>-<uuid>" by peeking
    // past the first separator when the leading char isn't hex.
    val lowChars = "0123".toSet
    val highChars = "cdef".toSet
    val hexShapeHits = valid.count { s =>
      val minChar = uuidRelevantHead(s.min.get)
      val maxChar = uuidRelevantHead(s.max.get)
      lowChars.contains(minChar) && highChars.contains(maxChar)
    }
    val hexShapeRatio = hexShapeHits.toDouble / valid.size
    notes += f"footer stats: uuid-domain-saturation=${hexShapeRatio}%.2f (${hexShapeHits}/${valid.size} files)"

    // Signal 2: Temporal correlation. Rank both instantTime and min, compute Spearman.
    val timedSamples = valid.filter(_.instantTime.nonEmpty)
    val temporalCorr =
      if (timedSamples.size < 3) 0.0
      else spearmanRankCorrelation(
        timedSamples.map(_.instantTime),
        timedSamples.map(_.min.get))
    notes += f"footer stats: temporal-correlation(instant vs min)=$temporalCorr%.3f"

    // Signal 3: Per-file range width — for UUIDs, min-prefix ≠ max-prefix in most files.
    // Uses the same "peek past separator" so prefix-namespaced UUIDs still register width.
    val widePerFile = valid.count { s =>
      uuidRelevantHead(s.min.get) != uuidRelevantHead(s.max.get)
    }
    val widePerFileRatio = widePerFile.toDouble / valid.size
    notes += f"footer stats: per-file-range-width=${widePerFileRatio}%.2f"

    // Decision. UUID-domain saturation is the strongest signal — if almost every
    // file spans low-hex-min and high-hex-max, the column is a random hash,
    // regardless of any accidental correlation with commit time. Hybrid
    // temporal-prefix + random-suffix keys are still classified as
    // TemporallyOrdered because that reflects how they behave in an ingestion
    // index (locality dominated by the time prefix).
    if (hexShapeRatio >= 0.9) {
      (KeyTypes.Random, "footer-stats-uuid-random", notes.toSeq)
    } else if (temporalCorr >= 0.7) {
      if (widePerFileRatio >= 0.5) {
        notes += "sample looks hybrid (temporal + random suffix); emitting TemporallyOrdered"
        (KeyTypes.TemporallyOrdered, "footer-stats-hybrid-temporal-random", notes.toSeq)
      } else {
        (KeyTypes.TemporallyOrdered, "footer-stats-monotonic", notes.toSeq)
      }
    } else if (hexShapeRatio >= 0.5) {
      // Moderate UUID signal but no monotonic trend — Random is the safer bet.
      (KeyTypes.Random, "footer-stats-random-weak-signal", notes.toSeq)
    } else {
      notes += "footer stats ambiguous — defaulting to Random"
      (KeyTypes.Random, "footer-stats-ambiguous", notes.toSeq)
    }
  }

  /**
   * Return the leading char most useful for UUID-domain detection. For a bare
   * value ("550e8400-...") this is just the first char lowercased. For a
   * prefix-namespaced value ("tenant42-550e8400-...") we skip past the first
   * `-`, `_`, or `:` separator and use the char after it — so the classifier
   * still sees `'5'` / `'f'` for the actual UUID content, rather than the
   * literal `'t'` from the tenant prefix.
   *
   * Only used for classification signals — the raw min/max are still used
   * elsewhere (Spearman correlation) so the temporal signal is unaffected.
   */
  private[lakeloader] def uuidRelevantHead(value: String): Char = {
    if (value.isEmpty) return ' '
    val first = value.charAt(0).toLower
    if (isHex(first)) return first
    // Not hex at position 0 — try just past the first separator, if any.
    val len = value.length
    val idxDash = value.indexOf('-')
    val idxUnd = value.indexOf('_')
    val idxCol = value.indexOf(':')
    val candidates = List(idxDash, idxUnd, idxCol).filter(_ >= 0)
    if (candidates.isEmpty) return first
    val sepIdx = candidates.min
    if (sepIdx + 1 >= len) first else value.charAt(sepIdx + 1).toLower
  }

  private def isHex(c: Char): Boolean =
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')

  /** Spearman rank correlation between two equal-length sequences of comparable values. */
  private[lakeloader] def spearmanRankCorrelation(a: Seq[String], b: Seq[String]): Double = {
    require(a.size == b.size, s"ranks require equal-length input: ${a.size} vs ${b.size}")
    val n = a.size
    if (n < 2) return 0.0
    def ranks(xs: Seq[String]): Seq[Double] = {
      val sorted = xs.zipWithIndex.sortBy(_._1)
      val ranked = new Array[Double](n)
      sorted.zipWithIndex.foreach { case ((_, origIdx), rankIdx) =>
        ranked(origIdx) = rankIdx.toDouble + 1.0
      }
      ranked.toIndexedSeq
    }
    val ra = ranks(a)
    val rb = ranks(b)
    val meanA = ra.sum / n
    val meanB = rb.sum / n
    var num = 0.0
    var denA = 0.0
    var denB = 0.0
    ra.zip(rb).foreach { case (x, y) =>
      val dx = x - meanA
      val dy = y - meanB
      num += dx * dy
      denA += dx * dx
      denB += dy * dy
    }
    val den = math.sqrt(denA * denB)
    if (den == 0.0) 0.0 else num / den
  }

  /** Legacy value-read classifier, used as a fallback when < 3 base files exist. */
  private def classifyKeyValueSamples(
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
      (KeyTypes.Random, "value-fallback-uuid-prefix", Some(keyCol), notes.toSeq)
    } else if (epochLike.toDouble / n > 0.9 || monotonic) {
      (KeyTypes.TemporallyOrdered, "value-fallback-epoch-or-monotonic", Some(keyCol), notes.toSeq)
    } else {
      notes += "sample looked ambiguous — defaulting to Random"
      (KeyTypes.Random, "value-fallback-ambiguous", Some(keyCol), notes.toSeq)
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
    writeText(fs, new Path(dir, "synth-derived.json"), renderDerivedJson(d, sourceTablePath))
  }

  /**
   * Machine-readable output consumed by WorkloadResizer. Hand-rolled JSON to
   * avoid pulling in a Jackson-flavored dependency that conflicts with the
   * Spark 3.5.3 jackson-databind pin.
   */
  private[lakeloader] def renderDerivedJson(d: DerivedConfig, sourceTablePath: String): String = {
    def escape(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")
    def q(s: String): String = "\"" + escape(s) + "\""
    def jsonList[T](xs: Seq[T], f: T => String): String = xs.map(f).mkString("[", ",", "]")
    val schemaJson = d.schemaChoice match {
      case SuppliedSchema(path) => s"""{"kind":"SuppliedSchema","path":${q(path)}}"""
      case InferredColumnCount(n) => s"""{"kind":"InferredColumnCount","numColumns":$n}"""
    }
    val round0Json = d.round0PartitionDistribution match {
      case Some(w) => jsonList(w, (x: Double) => x.toString)
      case None => "null"
    }
    val sb = new StringBuilder
    sb.append("{\n")
    sb.append(s"""  "sourceTablePath": ${q(sourceTablePath)},""").append("\n")
    sb.append(s"""  "numRounds": ${d.numRounds},""").append("\n")
    sb.append(s"""  "recordsPerRound": ${jsonList(d.recordsPerRound, (x: Long) => x.toString)},""").append("\n")
    sb.append(s"""  "medianRecordsPerRound": ${d.medianRecordsPerRound},""").append("\n")
    sb.append(s"""  "totalPartitions": ${d.totalPartitions},""").append("\n")
    sb.append(s"""  "updateRatio": ${d.updateRatio},""").append("\n")
    sb.append(s"""  "numPartitionsToUpdate": ${d.numPartitionsToUpdate},""").append("\n")
    sb.append(s"""  "recordSize": ${d.recordSize},""").append("\n")
    sb.append(s"""  "targetDataFileSize": ${d.targetDataFileSize},""").append("\n")
    sb.append(s"""  "updatePattern": ${q(d.updatePattern.toString)},""").append("\n")
    sb.append(s"""  "zipfShape": ${d.zipfShape},""").append("\n")
    sb.append(s"""  "partitionDistribution": ${jsonList(d.partitionDistribution, (x: Double) => x.toString)},""").append("\n")
    sb.append(s"""  "round0PartitionDistribution": $round0Json,""").append("\n")
    sb.append(s"""  "keyType": ${q(d.keyType.toString)},""").append("\n")
    sb.append(s"""  "keyTypeSource": ${q(d.keyTypeSource)},""").append("\n")
    sb.append(s"""  "recordKeyField": ${d.recordKeyField.map(q).getOrElse("null")},""").append("\n")
    sb.append(s"""  "schemaChoice": $schemaJson,""").append("\n")
    sb.append(s"""  "rliMode": ${q(d.rliMode)}""").append("\n")
    sb.append("}\n")
    sb.toString
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

    val schemaLine = d.schemaChoice match {
      case SuppliedSchema(path) => s"--avro-schema $path"
      case InferredColumnCount(n) => s"--number-columns $n"
    }

    val base = Seq(
      "--path <fill-in>",
      schemaLine,
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
      s"  schemaChoice=${d.schemaChoice match {
        case SuppliedSchema(p) => s"SuppliedSchema($p)"
        case InferredColumnCount(n) => s"InferredColumnCount(numColumns=$n)"
      }}",
      s"  rliMode=${d.rliMode}",
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
