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

    val (schemaChoice, schemaNotes) = resolveSchemaChoice(metaClient, config)

    val derived = deriveConfig(
      commits, config, keyType, keyTypeSource, recordKeyField, schemaChoice, keyTypeNotes ++ schemaNotes)

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
      schemaChoice: SchemaChoice,
      auditNotesPrefix: Seq[String] = Nil): DerivedConfig = {

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

    val auditNotes = auditNotesPrefix ++ Seq(
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
      schemaChoice = schemaChoice,
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
    sb.append(s"""  "schemaChoice": $schemaJson""").append("\n")
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
