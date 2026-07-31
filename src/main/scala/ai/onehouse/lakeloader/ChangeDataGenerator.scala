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

import ai.onehouse.lakeloader.ChangeDataGenerator.{genExactParallelRDD, genParallelRDD, COMPRESSION_RATIO_GUESS, PARTITION_PATH_FIELD_NAME, RECORD_KEY_FIELD_NAME}
import ai.onehouse.lakeloader.configs.{CommitSpec, DatagenConfig, ExternalBootstrapSpec, FineGrainedWorkloadSpec, KeyTypes, PartitionDistributionSpec, UpdatePatterns}
import ai.onehouse.lakeloader.configs.KeyTypes.KeyType
import ai.onehouse.lakeloader.configs.UpdatePatterns.{Uniform, UpdatePatterns, Zipf}
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser
import ai.onehouse.lakeloader.utils.{AvroSchemaUtils, ComplexDataGenerator, MathUtils, StringUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CatalystUtil.partitionLocalLimit
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import ai.onehouse.lakeloader.utils.StringUtils.lineSepBold

import java.io.Serializable
import java.time.LocalDate
import java.util.UUID.randomUUID

import scala.util.Random

/**
 * Class that can generates the workload based on advanced criteria like insert vs update ratios & update
 * and insert patterns (spread across partitions).
 *
 * @param spark     Spark's session
 * @param numRounds number of runs of workload generation and the measured operation
 */
class ChangeDataGenerator(val spark: SparkSession, val numRounds: Int = 10) extends Serializable {

  private val SEED: Long = 378294793957830L

  import spark.implicits._

  // Currently only supports flat schema.
  private def getSchema(numFields: Int = 10): StructType = {
    // First 4 fields are fixed: primary key, partition key, round id, and timestamp.
    val fields = Seq(
      StructField("key", StringType, nullable = false),
      StructField("partition", StringType, nullable = false),
      StructField("round", IntegerType, nullable = false),
      StructField("ts", LongType, nullable = false)) ++ (0 until numFields - 4)
      .map(i => {
        i % 10 match {
          case 0 => StructField(s"textField1$i", StringType, nullable = true)
          case 1 => StructField(s"textField2$i", StringType, nullable = true)
          case 2 => StructField(s"textField3$i", StringType, nullable = true)
          case 3 => StructField(s"textField4$i", StringType, nullable = true)
          case 4 => StructField(s"textField5$i", StringType, nullable = true)
          case 5 => StructField(s"longField1$i", LongType, nullable = true)
          case 6 => StructField(s"decimalField$i", FloatType, nullable = true)
          case 7 => StructField(s"longField2$i", LongType, nullable = true)
          case 8 => StructField(s"longField3$i", LongType, nullable = true)
          case 9 => StructField(s"intField1$i", IntegerType, nullable = true)
        }
      })
    StructType(fields)
  }

  private def generateNewRecord(
      round: Int,
      size: Int,
      partitionPaths: List[String],
      partitionDistributionCDF: List[Double],
      keyType: KeyType,
      schema: StructType,
      random: Random,
      nullifyDataFields: Boolean = false) = {
    ComplexDataGenerator.generateRow(
      schema,
      round,
      partitionPaths,
      partitionDistributionCDF,
      keyType,
      size,
      random,
      nullifyDataFields)
  }

  /**
   * Deterministic per-task RNG. Every Spark task deserializes an identical copy of this
   * instance, so sharing the closure-captured `this.random` makes all tasks of a stage emit
   * IDENTICAL value sequences. That duplication is invisible in the row counts but parquet
   * dictionary encoding dedupes the repeats, silently deflating on-disk sizes (and the
   * record-size estimate) by up to the stage parallelism. Seed by (round, partition index)
   * instead: distinct data per task, still fully reproducible across reruns. The round occupies
   * the high 32 bits of the seed offset so no (round, partitionIndex) pair can collide with
   * another.
   */
  private def taskRandom(round: Int, partitionIndex: Int): Random =
    new Random(SEED + (round.toLong << 32) + partitionIndex)

  /**
   * Executes the spark DAG to generate the workload ahead of time, for the configured number of rounds.
   *
   * @param path                           path to place generated input local_workloads at
   * @param roundsDistribution             total number of records to generate per round
   * @param numColumns                      total number of columns in the schema
   * @param recordSize                     size of each record in bytes
   * @param updateRatio                    ratio of updates in the batch (remaining will be inserts)
   * @param totalPartitions                Number of total partitions (default: unpartitioned)
   * @param partitionDistributionMatrixOpt defines to-be-generated new records' distribution across partitions (for every round)
   * @param targetDataFileSize             data file size hint that data generation will aim to produce
   * @param skipIfExists                   should skip generation for the rounds possibly generated during previous
   * @param keyType                        format for generating the primary key
   * @param startRound                     round to start generating from, default 0.
   * @param updatePatterns                 Update pattern for generating updates: random (uniform) or zipf (skewed).
   * @param numPartitionsToUpdate          Number of partitions to update (default -1/ none)
   */
  def generateWorkload(
      path: String,
      roundsDistribution: List[Long] = List.fill(numRounds)(1000000L),
      numColumns: Int = 10,
      recordSize: Int = 1024,
      updateRatio: Double = 0.5f,
      totalPartitions: Int = -1,
      partitionDistributionMatrixOpt: Option[List[List[Double]]] = None,
      targetDataFileSize: Int = 128 * 1024 * 1024,
      skipIfExists: Boolean = false,
      keyType: KeyType = KeyTypes.Random,
      startRound: Int = 0,
      updatePatterns: UpdatePatterns = UpdatePatterns.Uniform,
      numPartitionsToUpdate: Int = -1,
      zipfianShape: Double = 2.93,
      avroSchemaPath: Option[String] = None): Unit = {
    require(path.nonEmpty, "Path cannot be empty")
    require(
      totalPartitions != -1 || partitionDistributionMatrixOpt.isDefined,
      "Either set the total partitions or configure the partitionDistributionMatrixOpt")
    if (avroSchemaPath.isEmpty) {
      require(
        numColumns >= 5,
        "The number of columns needs to be at least 5 since we need at least 4 cols for key, partition, round, and timestamp.")
    }
    require(
      numPartitionsToUpdate <= totalPartitions,
      "The number of partitions to update should be lower than the total partitions")

    // Compute records distribution matrix across partitions; such matrix
    // could be explicitly provided as an optional parameter prescribing corresponding
    // distribution for every round
    val (targetPartitionsCount, computedPartitionDistMatrix) =
      genPartitionsDistributionMatrix(totalPartitions, partitionDistributionMatrixOpt)

    val partitionPaths = genDateBasedPartitionValues(targetPartitionsCount)
    val schema = avroSchemaPath match {
      case Some(schemaPath) =>
        AvroSchemaUtils.loadSchemaFromAvscFile(schemaPath, spark.sparkContext.hadoopConfiguration)
      case None =>
        getSchema(numColumns)
    }

    // Lazy: with a custom Avro schema the sizing does a ~20MB sample write; only pay for it
    // when some round actually generates (a fully skipped --skip-if-exists rerun never does).
    lazy val effectiveSizing =
      resolveEffectiveSizing(path, schema, partitionPaths, recordSize, keyType, avroSchemaPath)

    ////////////////////////////////////////
    // Generating workload
    ////////////////////////////////////////

    (startRound until startRound + numRounds).foreach(curRound => {
      val targetLocation = s"$path/$curRound"
      val partitionDistribution = computedPartitionDistMatrix(curRound)
      // Compute CDF for corresponding records distribution across partitions (for subsequent sampling)
      val partitionDistributionCDF = MathUtils.makeCDF(partitionDistribution)

      val targetLocationPath = new Path(targetLocation)
      val fs = targetLocationPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

      if (skipIfExists && fs
          .exists(targetLocationPath) && fs.listFiles(targetLocationPath, false).hasNext) {
        println(s"Skipping generation for round # $curRound, location $targetLocation is not empty")
      } else {
        // Calculate inserts/updates split
        val targetRecords = roundsDistribution(curRound)
        val numUpdates =
          if (curRound == 0 || numPartitionsToUpdate <= 0) 0
          else Math.min((updateRatio * targetRecords).toLong, curRound * targetRecords)
        val numInserts = targetRecords - numUpdates

        // Use ceiling so the per-file size never exceeds targetDataFileSize. With floor,
        // 7.67 truncates to 7 and each file overshoots the cap; ceil → 8 keeps every file
        // strictly under the configured target (default 128 MB).
        val (effectiveRecordSize, effectiveCompressionRatio) = effectiveSizing
        val estimatedTotalBytes =
          targetRecords.toDouble * effectiveRecordSize * effectiveCompressionRatio
        val targetParallelism =
          Math.max(2, Math.ceil(estimatedTotalBytes / targetDataFileSize).toInt)

        println(s"""
             |$lineSepBold
             |Round # $curRound: numInserts $numInserts, numUpdates $numUpdates
             |Creating at $targetLocation
             |$lineSepBold
             |""".stripMargin)

        ////////////////////////////////////////
        // Generating inserts
        ////////////////////////////////////////
        val insertsRDD = genParallelRDD(spark, targetParallelism, 0, numInserts)
          .mapPartitionsWithIndex { (partIdx, it) =>
            val random = taskRandom(curRound, partIdx)
            it.map(_ =>
              generateNewRecord(
                curRound,
                recordSize,
                partitionPaths,
                partitionDistributionCDF,
                keyType,
                schema,
                random))
          }

        val insertsDF = spark.createDataFrame(insertsRDD, schema)
        val upsertDF =
          if (numUpdates == 0) insertsDF
          else
            // unionByName: the update path's key-join reorders columns (join keys first), so a
            // positional union would silently misalign — or fail on — schemas whose key/partition/
            // round fields are not the leading columns (e.g. custom Avro schemas).
            insertsDF.unionByName(
              generateUpdates(
                updatePatterns,
                partitionPaths,
                numUpdates,
                numPartitionsToUpdate,
                path,
                targetParallelism,
                curRound,
                zipfianShape))

        spark.time {
          upsertDF
            .repartition(targetParallelism)
            .write
            .format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
            .mode(SaveMode.Overwrite)
            .save(targetLocation)
        }

        spark.catalog.clearCache()
      }
    })
  }

  /**
   * Generates a workload from a fine-grained spec that prescribes *exact* per-partition
   * insert/update counts for every commit, mimicking a real table's commit history.
   *
   * Round numbering: round 0 is the bootstrap — `spec.bootstrap.totalRecords` inserts spread
   * evenly across one partition per day in `[startDate, endDate]`. Round k (k >= 1) replays
   * `spec.commits(k - 1)`: for each listed partition, exactly the requested number of inserts
   * plus exactly the requested number of updates, the latter sampled uniformly at random from
   * the latest version of the keys already present in that partition (from rounds < k only).
   *
   * When `spec.externalBootstrap` is set instead of `spec.bootstrap`, round 0 is never generated
   * (generation starts at round 1, per `spec.startRound`) and updates instead sample real
   * `_hoodie_record_key` values back from the pre-populated external Hudi table named by the
   * spec. See [[generateExternalBootstrapRound]].
   *
   * Unlike [[generateWorkload]] (which samples partitions from a probability distribution),
   * this path produces deterministic, exact per-partition record counts.
   *
   * Note: the number of rounds is derived from the spec (1 + spec.commits.size); the
   * constructor's `numRounds` is not used here.
   *
   * @param path               path to place generated input workloads at
   * @param spec               fine-grained workload spec (bootstrap + per-commit counts)
   * @param numColumns         total number of columns in the default schema (ignored with a custom Avro schema)
   * @param recordSize         size of each record in bytes (width hint with a custom Avro schema)
   * @param targetDataFileSize data file size hint that data generation will aim to produce
   * @param skipIfExists       should skip generation for the rounds possibly generated during previous runs
   * @param keyType            format for generating the primary key
   * @param startRound         round to start generating from (for resuming); pass -1 (default) to
   *                           use `spec.startRound` (0 for a normal bootstrap spec, 1 for external
   *                           bootstrap)
   * @param avroSchemaPath     optional custom Avro schema (.avsc) to generate data for
   */
  def generateFineGrainedWorkload(
      path: String,
      spec: FineGrainedWorkloadSpec,
      numColumns: Int = 10,
      recordSize: Int = 1024,
      targetDataFileSize: Int = 128 * 1024 * 1024,
      skipIfExists: Boolean = false,
      keyType: KeyType = KeyTypes.Random,
      startRound: Int = -1,
      avroSchemaPath: Option[String] = None): Unit = {
    require(path.nonEmpty, "Path cannot be empty")
    val effectiveStartRound = if (startRound == -1) spec.startRound else startRound
    val totalRounds = spec.totalRounds
    require(
      effectiveStartRound >= spec.startRound && effectiveStartRound < totalRounds,
      s"startRound must be within [${spec.startRound}, $totalRounds), got $effectiveStartRound")
    if (avroSchemaPath.isEmpty) {
      require(
        numColumns >= 5,
        "The number of columns needs to be at least 5 since we need at least 4 cols for key, partition, round, and timestamp.")
    }

    // With externalBootstrap, there is no bootstrap round to derive representative partitions
    // from; fall back to every partition referenced anywhere in the commits (only used as a
    // sizing-sample seed and, in the non-external path, as round 0's partition list).
    val representativePartitions = spec.bootstrap match {
      case Some(bootstrap) => bootstrap.partitionValues
      case None =>
        val fromCommits = spec.commits.flatMap(_.partitionOps.map(_._1)).distinct
        require(
          fromCommits.nonEmpty,
          "externalBootstrap requires at least one commit with a touched partition")
        fromCommits
    }
    val schema = avroSchemaPath match {
      case Some(schemaPath) =>
        AvroSchemaUtils.loadSchemaFromAvscFile(schemaPath, spark.sparkContext.hadoopConfiguration)
      case None =>
        getSchema(numColumns)
    }
    // Lazy: with a custom Avro schema the sizing does a ~20MB sample write; only pay for it
    // when some round actually generates (a fully skipped --skip-if-exists rerun never does).
    lazy val effectiveSizing = resolveEffectiveSizing(
      path,
      schema,
      representativePartitions,
      recordSize,
      keyType,
      avroSchemaPath)

    // Built once, reused by every round: see generateExternalBootstrapRound's scaladoc for why a
    // single shared pool (rather than one per partition or per commit) is sufficient.
    lazy val externalBootstrapPool: (DataFrame, Long) = buildExternalBootstrapPayloadPool(
      spec.externalBootstrap.get,
      spec.commits,
      recordSize,
      keyType,
      schema,
      targetDataFileSize)

    (effectiveStartRound until totalRounds).foreach(curRound => {
      val targetLocation = s"$path/$curRound"
      val targetLocationPath = new Path(targetLocation)
      val fs = targetLocationPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

      if (skipIfExists && fs
          .exists(targetLocationPath) && fs.listFiles(targetLocationPath, false).hasNext) {
        println(s"Skipping generation for round # $curRound, location $targetLocation is not empty")
      } else if (spec.externalBootstrap.isDefined) {
        // curRound is always >= 1 here: startRound defaults to 1 (spec.startRound) whenever
        // externalBootstrap is set, so round 0 (bootstrap) is never reached.
        val ext = spec.externalBootstrap.get
        val commit = spec.commits(curRound - 1)
        val numInserts = commit.partitionOps.collect { case (_, ops) if ops.inserts > 0 => ops.inserts }.sum
        val numUpdates = commit.partitionOps.collect { case (_, ops) if ops.updates > 0 => ops.updates }.sum
        val (effectiveRecordSize, effectiveCompressionRatio) = effectiveSizing
        val estimatedTotalBytes =
          (numInserts + numUpdates).toDouble * effectiveRecordSize * effectiveCompressionRatio
        val targetParallelism =
          Math.max(2, Math.ceil(estimatedTotalBytes / targetDataFileSize).toInt)
        val (payloadPoolDF, poolSize) = externalBootstrapPool

        println(s"""
             |$lineSepBold
             |Round # $curRound: numInserts $numInserts, numUpdates $numUpdates
             |(external bootstrap: ${ext.tablePath})
             |Creating at $targetLocation
             |$lineSepBold
             |""".stripMargin)

        val (roundDF, persistedIntermediates) = generateExternalBootstrapRound(
          curRound,
          commit,
          ext,
          payloadPoolDF,
          poolSize,
          schema,
          keyType,
          targetParallelism)

        spark.time {
          roundDF
            .repartition(targetParallelism)
            .write
            .format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
            .mode(SaveMode.Overwrite)
            .save(targetLocation)
        }
        persistedIntermediates.foreach(_.unpersist())
        spark.catalog.clearCache()
      } else {
        val (insertCounts, updateCounts) =
          if (curRound == 0) {
            (
              evenSplit(representativePartitions, spec.bootstrap.get.totalRecords),
              Seq.empty[(String, Long)])
          } else {
            val commit = spec.commits(curRound - 1)
            (
              commit.partitionOps.collect { case (p, ops) if ops.inserts > 0 => (p, ops.inserts) },
              commit.partitionOps.collect { case (p, ops) if ops.updates > 0 => (p, ops.updates) })
          }
        val numInserts = insertCounts.map(_._2).sum
        val numUpdates = updateCounts.map(_._2).sum

        // Use ceiling so the per-file size never exceeds targetDataFileSize (see generateWorkload).
        val (effectiveRecordSize, effectiveCompressionRatio) = effectiveSizing
        val estimatedTotalBytes =
          (numInserts + numUpdates).toDouble * effectiveRecordSize * effectiveCompressionRatio
        val targetParallelism =
          Math.max(2, Math.ceil(estimatedTotalBytes / targetDataFileSize).toInt)

        println(s"""
             |$lineSepBold
             |Round # $curRound: numInserts $numInserts, numUpdates $numUpdates (exact per-partition spec)
             |Inserts: ${if (insertCounts.isEmpty) "none"
                  else insertCounts.map(t => s"${t._1}=${t._2}").mkString(", ")}
             |Updates: ${if (updateCounts.isEmpty) "none"
                  else updateCounts.map(t => s"${t._1}=${t._2}").mkString(", ")}
             |Creating at $targetLocation
             |$lineSepBold
             |""".stripMargin)

        val insertsDF =
          generateExactInserts(
            curRound,
            insertCounts,
            recordSize,
            keyType,
            schema,
            targetParallelism,
            spec.nullifiedPartitions)
        var persistedIntermediates: Seq[DataFrame] = Seq.empty
        val upsertDF =
          if (numUpdates == 0) insertsDF
          else {
            // Updates must reflect the table state as of the previous commit: sample only from
            // rounds strictly before this one (a plain `$path/*` glob could pick up stale data
            // from later rounds when re-generating with startRound in the middle).
            val priorRoundPaths = (0 until curRound).map(r => s"$path/$r")
            val (rawUpdatesDF, persisted) =
              getExactPerPartitionUpdates(updateCounts, priorRoundPaths, curRound)
            persistedIntermediates = persisted
            insertsDF.unionByName(
              regenerateUpdateValues(rawUpdatesDF, curRound, spec.nullifiedPartitions))
          }

        spark.time {
          upsertDF
            .repartition(targetParallelism)
            .write
            .format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
            .mode(SaveMode.Overwrite)
            .save(targetLocation)
        }

        // The update picker's cached intermediates are only consumed by the write above;
        // release them promptly so long many-round runs don't accumulate storage memory.
        persistedIntermediates.foreach(_.unpersist())
        spark.catalog.clearCache()
      }
    })
  }

  /**
   * Split `totalRecords` evenly across the given partitions; the first
   * `totalRecords % partitions.size` partitions absorb the remainder (one extra record each).
   */
  private def evenSplit(partitions: List[String], totalRecords: Long): Seq[(String, Long)] = {
    val base = totalRecords / partitions.size
    val remainder = totalRecords % partitions.size
    partitions.zipWithIndex.map { case (partition, idx) =>
      (partition, base + (if (idx < remainder) 1 else 0))
    }
  }

  /**
   * Generate exactly the requested number of insert rows per partition. Each generated row's
   * global index is mapped deterministically onto a partition via the cumulative count
   * boundaries — unlike the CDF-sampling path, per-partition counts are exact, not expected
   * values.
   */
  private def generateExactInserts(
      curRound: Int,
      insertCounts: Seq[(String, Long)],
      recordSize: Int,
      keyType: KeyType,
      schema: StructType,
      targetParallelism: Int,
      nullifiedPartitions: Set[String] = Set.empty): DataFrame = {
    // Drop zero-count entries (e.g. bootstrap totalRecords < numPartitions): they would create
    // duplicate cumulative boundaries below, and binarySearch makes no guarantee which duplicate
    // it returns — a row could get misassigned to a partition specced for 0 records. Filtering
    // keeps the boundaries strictly increasing, making the lookup unambiguous.
    val positiveCounts = insertCounts.filter(_._2 > 0)
    val totalInserts = positiveCounts.map(_._2).sum
    if (totalInserts == 0) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }
    // Singleton partition list per entry so generateRow's CDF sampling degenerates to a fixed pick.
    val partitionSingletons = positiveCounts.map(t => List(t._1)).toArray
    val nullifyFlags = positiveCounts.map(t => nullifiedPartitions.contains(t._1)).toArray
    val cumulativeEnds = positiveCounts.map(_._2).scanLeft(0L)(_ + _).tail.toArray
    val singletonCDF = List(1.0)

    val insertsRDD = genExactParallelRDD(spark, targetParallelism, totalInserts)
      .mapPartitionsWithIndex { (partIdx, it) =>
        val random = taskRandom(curRound, partIdx)
        it.map { globalIdx =>
          // Partition i owns global indices [cumulativeEnds(i-1), cumulativeEnds(i)); find the
          // smallest i with cumulativeEnds(i) > globalIdx.
          val searched = java.util.Arrays.binarySearch(cumulativeEnds, globalIdx + 1)
          val partitionIdx = if (searched >= 0) searched else -searched - 1
          generateNewRecord(
            curRound,
            recordSize,
            partitionSingletons(partitionIdx),
            singletonCDF,
            keyType,
            schema,
            random,
            nullifyFlags(partitionIdx))
        }
      }
    spark.createDataFrame(insertsRDD, schema)
  }

  /**
   * Generate exactly the requested number of *new* keys per partition, for the external-bootstrap
   * path's inserts. Same exact-count RDD mapping as [[generateExactInserts]], but only produces
   * `(key, partition)` pairs — the payload is attached later from the shared pool via
   * [[generateExternalBootstrapRound]].
   *
   * @param suffixKeyWithPartitionPath when true, generate `<uuid>-<partition>` keys instead of
   *                                   the normal `keyType`-based scheme (see
   *                                   [[ExternalBootstrapSpec.suffixKeyWithPartitionPath]]).
   */
  private def generateExactInsertKeys(
      curRound: Int,
      insertCounts: Seq[(String, Long)],
      keyType: KeyType,
      targetParallelism: Int,
      suffixKeyWithPartitionPath: Boolean = false): DataFrame = {
    val keySchema = StructType(
      Seq(
        StructField("key", StringType, nullable = false),
        StructField("partition", StringType, nullable = false)))
    val positiveCounts = insertCounts.filter(_._2 > 0)
    val totalInserts = positiveCounts.map(_._2).sum
    if (totalInserts == 0) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], keySchema)
    }
    val partitionValues = positiveCounts.map(_._1).toArray
    val cumulativeEnds = positiveCounts.map(_._2).scanLeft(0L)(_ + _).tail.toArray

    val keysRDD = genExactParallelRDD(spark, targetParallelism, totalInserts)
      .mapPartitionsWithIndex { (partIdx, it) =>
        val random = taskRandom(curRound, partIdx)
        it.map { globalIdx =>
          val searched = java.util.Arrays.binarySearch(cumulativeEnds, globalIdx + 1)
          val partitionIdx = if (searched >= 0) searched else -searched - 1
          val partitionValue = partitionValues(partitionIdx)
          val key =
            if (suffixKeyWithPartitionPath) s"${randomUUID()}_$partitionValue"
            else ComplexDataGenerator.generateKey(keyType, curRound, System.currentTimeMillis(), random)
          Row(key, partitionValue)
        }
      }
    spark.createDataFrame(keysRDD, keySchema)
  }

  /**
   * Attach a dense sequential index column (0 until df.count()) to every row of `df`, via
   * `RDD.zipWithIndex` (cheap, no shuffle) rather than a global `row_number()` window (which
   * forces a single-partition sort). Used to pair up two independently-generated DataFrames of
   * equal size row-for-row via an equi-join on the index column.
   */
  private def zipWithSequentialIndex(df: DataFrame, indexCol: String): DataFrame = {
    val indexedSchema = df.schema.add(indexCol, LongType, nullable = false)
    val indexedRDD = df.rdd.zipWithIndex().map { case (row, idx) => Row.fromSeq(row.toSeq :+ idx) }
    spark.createDataFrame(indexedRDD, indexedSchema)
  }

  /**
   * Sample exactly `n` rows (with replacement) from `indexedPoolDF` (a DataFrame carrying a dense
   * `_pool_idx` column spanning `0 until poolSize`, as produced by
   * [[buildExternalBootstrapPayloadPool]]). `n` is typically far larger than `poolSize` when
   * summed across every partition touched in a commit, so replacement is required — only a
   * single partition's own draw within one commit is effectively distinct (bounded by
   * `payloadPoolMultiplier`, see the spec doc).
   */
  private def samplePoolRows(
      indexedPoolDF: DataFrame,
      poolSize: Long,
      n: Long,
      seed: Long): DataFrame = {
    val idxDF = spark.range(n).select((rand(seed) * poolSize).cast(LongType).as("_pool_idx"))
    idxDF.join(indexedPoolDF, "_pool_idx").drop("_pool_idx")
  }

  /**
   * Build the single payload pool shared by every commit/partition in an externalBootstrap run.
   * Payload rows carry every schema field except the identity fields (key/partition/round/ts,
   * see [[ComplexDataGenerator.IDENTITY_FIELDS]]) — those are stitched on per-round by
   * [[generateExternalBootstrapRound]] via [[ComplexDataGenerator.attachIdentity]].
   *
   * Sized to `payloadPoolMultiplier` times the single largest (inserts + updates) demand across
   * every (partition, commit) pair in the whole spec: since payload generation doesn't depend on
   * partition or commit, a pool that size covers any single commit's per-partition draw with
   * headroom, and is reused (via sampling with replacement) across every other commit/partition
   * instead of being regenerated — the win grows with how many commits touch the same hot
   * partitions.
   */
  private def buildExternalBootstrapPayloadPool(
      ext: ExternalBootstrapSpec,
      commits: List[CommitSpec],
      recordSize: Int,
      keyType: KeyType,
      schema: StructType,
      targetDataFileSize: Int): (DataFrame, Long) = {
    val maxSingleDemand =
      commits.flatMap(_.partitionOps.map { case (_, ops) => ops.inserts + ops.updates }).max
    val poolSize = Math.ceil(ext.payloadPoolMultiplier * maxSingleDemand).toLong

    println(s"""
         |$lineSepBold
         |Building external-bootstrap payload pool: $poolSize rows
         |(${ext.payloadPoolMultiplier}x max single-partition-per-commit demand of $maxSingleDemand),
         |reused across all ${commits.size} commits.
         |$lineSepBold
         |""".stripMargin)

    val parallelism = Math.max(
      2,
      Math.ceil(poolSize.toDouble * recordSize * COMPRESSION_RATIO_GUESS / targetDataFileSize).toInt)
    // Round 0 is otherwise unused whenever externalBootstrap is set (generation starts at round
    // 1), so it's a safe, non-colliding round number to seed this one-off pool generation with.
    val fullRowsDF =
      generateExactInserts(0, Seq(("__payload_pool__", poolSize)), recordSize, keyType, schema, parallelism)
    val payloadOnlyDF = fullRowsDF.drop(ComplexDataGenerator.IDENTITY_FIELDS.toSeq: _*)
    val indexed = zipWithSequentialIndex(payloadOnlyDF, "_pool_idx").persist()
    indexed.count() // materialize once; every round's sampling join below reuses this persisted result
    (indexed, poolSize)
  }

  /**
   * Generate one round's worth of records for the externalBootstrap path: exact per-partition
   * insert/update counts (from `commit`, identical semantics to the normal path), but with
   * inserts freshly-keyed (no prior lake-loader-generated data to draw from) and updates sampled
   * from real keys read back from the pre-populated external Hudi table, rather than from an
   * earlier round's parquet output.
   *
   * Returns the round's DataFrame plus any persisted intermediates the caller must unpersist once
   * the round has been written (matches the calling convention of [[getExactPerPartitionUpdates]]).
   */
  private def generateExternalBootstrapRound(
      curRound: Int,
      commit: CommitSpec,
      ext: ExternalBootstrapSpec,
      payloadPoolDF: DataFrame,
      poolSize: Long,
      schema: StructType,
      keyType: KeyType,
      targetParallelism: Int): (DataFrame, Seq[DataFrame]) = {
    val insertCounts = commit.partitionOps.collect { case (p, ops) if ops.inserts > 0 => (p, ops.inserts) }
    val updateCounts = commit.partitionOps.collect { case (p, ops) if ops.updates > 0 => (p, ops.updates) }
    val numInserts = insertCounts.map(_._2).sum
    val numUpdates = updateCounts.map(_._2).sum
    val keySchema = StructType(
      Seq(
        StructField("key", StringType, nullable = false),
        StructField("partition", StringType, nullable = false)))

    val insertKeysDF = generateExactInsertKeys(
      curRound,
      insertCounts,
      keyType,
      targetParallelism,
      ext.suffixKeyWithPartitionPath)
    val (updateKeysDF, updatePersisted) =
      if (numUpdates == 0) {
        (spark.createDataFrame(spark.sparkContext.emptyRDD[Row], keySchema), Seq.empty[DataFrame])
      } else {
        val picked = getExactPerPartitionUpdatesFromHudi(ext, updateCounts, curRound)
        (picked.select("key", "partition"), Seq(picked))
      }

    val identityDF = insertKeysDF.unionByName(updateKeysDF)
    val identityIndexed = zipWithSequentialIndex(identityDF, "_ridx")

    val ts = System.currentTimeMillis()
    val sampledPayload =
      samplePoolRows(payloadPoolDF, poolSize, numInserts + numUpdates, SEED + curRound)
    val sampledPayloadIndexed = zipWithSequentialIndex(sampledPayload, "_ridx")

    val joined = identityIndexed.join(sampledPayloadIndexed, "_ridx")
    val finalDF = joined.select(schema.fields.map { field =>
      field.name match {
        case "round" => lit(curRound).as("round")
        case "ts" => lit(ts).as("ts")
        case name => col(name)
      }
    }: _*)
    (finalDF, updatePersisted)
  }

  /**
   * Like [[getExactPerPartitionUpdates]], but sources candidate keys from a pre-populated
   * external Hudi table instead of previously-generated round directories: reads
   * `recordKeyField`/`partitionPathField` (partition- and column-pruned; works with or without
   * the Record Level Index, since Spark's ordinary partition pruning plus Parquet column pruning
   * apply either way), then applies the exact same stratified-sample-then-trim technique to hit
   * exact per-partition counts. No `rank()`-by-round de-duplication is needed here (unlike the
   * normal path) since the Hudi table already holds only the latest version of each key.
   */
  private def getExactPerPartitionUpdatesFromHudi(
      ext: ExternalBootstrapSpec,
      updateCounts: Seq[(String, Long)],
      currentRound: Int): DataFrame = {
    val partitionsToUpdate = updateCounts.map(_._1)
    println(
      s"Reading existing keys from external Hudi table '${ext.tablePath}' for round # $currentRound: " +
        updateCounts.map(t => s"${t._1}=${t._2}").mkString(", "))

    val sourceDf = spark.read
      .format("hudi")
      .load(ext.tablePath)
      .select(col(ext.partitionPathField).as("partition"), col(ext.recordKeyField).as("key"))
      .filter(col("partition").isin(partitionsToUpdate: _*))
    sourceDf.persist()

    val availableCounts: Map[String, Long] = sourceDf
      .groupBy("partition")
      .count()
      .collect()
      .map(row => row.getAs[String]("partition") -> row.getAs[Long]("count"))
      .toMap

    val effectiveCounts: Seq[(String, Long, Long)] = updateCounts.map { case (partition, desired) =>
      val available = availableCounts.getOrElse(partition, 0L)
      require(
        available > 0,
        s"Round # $currentRound requests $desired updates for partition '$partition', but no " +
          s"keys exist for it in external Hudi table '${ext.tablePath}'")
      if (available < desired) {
        println(
          s"WARN: Round # $currentRound requests $desired updates for partition '$partition' " +
            s"but only $available distinct keys exist in the external table; updating all $available")
      }
      (partition, Math.min(desired, available), available)
    }

    val sampleFractions: Map[String, Double] = effectiveCounts.map {
      case (partition, desired, available) =>
        val fraction =
          if (desired >= available) 1.0
          else Math.min(1.0, (desired * 1.2 + 100.0) / available)
        partition -> fraction
    }.toMap
    val sampledDF = sourceDf.stat.sampleBy("partition", sampleFractions, SEED + currentRound)

    val desiredDF = effectiveCounts
      .map { case (partition, desired, _) => (partition, desired) }
      .toDF("partition", "desired_updates")
    val perPartitionWindow = Window.partitionBy("partition").orderBy(rand(SEED + currentRound))
    val pickedDF = sampledDF
      .withColumn("rn", row_number().over(perPartitionWindow))
      .join(broadcast(desiredDF), "partition")
      .filter($"rn" <= $"desired_updates")
      .drop("rn", "desired_updates")
    pickedDF.persist()
    sourceDf.unpersist()

    val pickedCounts: Map[String, Long] = pickedDF
      .groupBy("partition")
      .count()
      .collect()
      .map(row => row.getAs[String]("partition") -> row.getAs[Long]("count"))
      .toMap
    effectiveCounts.foreach { case (partition, desired, _) =>
      val actual = pickedCounts.getOrElse(partition, 0L)
      if (actual != desired) {
        println(
          s"WARN: Round # $currentRound picked $actual update keys for partition '$partition' " +
            s"instead of the requested $desired (over-sample undershoot)")
      }
    }
    pickedDF
  }

  /**
   * Pick exactly `updateCounts(partition)` keys per partition to update, sampled uniformly at
   * random from the latest version of each key currently in that partition. Returns the
   * full-width rows for the picked keys (values still need regeneration via
   * [[regenerateUpdateValues]]).
   *
   * Exactness: a cheap stratified over-sample (via `sampleBy`) first shrinks each partition's
   * candidate set, then a per-partition `row_number` trims to the exact requested count. If a
   * partition holds fewer keys than requested, all of its keys are updated and a warning is
   * logged.
   *
   * Returns the raw update rows plus the persisted intermediates backing them; the caller must
   * unpersist those once the round consuming the rows has been written.
   */
  private def getExactPerPartitionUpdates(
      updateCounts: Seq[(String, Long)],
      priorRoundPaths: Seq[String],
      currentRound: Int): (DataFrame, Seq[DataFrame]) = {
    val partitionsToUpdate = updateCounts.map(_._1)
    println(
      s"Generating exact per-partition updates for round # $currentRound: " +
        updateCounts.map(t => s"${t._1}=${t._2}").mkString(", "))

    var sourceDf = spark.read
      .format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
      .load(priorRoundPaths: _*)
    sourceDf = sourceDf.filter(col("partition").isin(partitionsToUpdate: _*))
    sourceDf.select("key", "partition", "round").createOrReplaceTempView("source_df_partitions")

    // Rank on the narrow (key, partition, round) projection only (see getRandomlyDistributedUpdates).
    var rankedDF = spark.sql("""
        | SELECT key, partition, `round`, rank(key) OVER (PARTITION BY key ORDER BY round DESC) as key_rank
        | FROM source_df_partitions
        |""".stripMargin)
    rankedDF = rankedDF.filter($"key_rank" === 1).drop(s"key_rank")
    rankedDF.persist()

    val availableCounts: Map[String, Long] = rankedDF
      .groupBy("partition")
      .count()
      .collect()
      .map(row => row.getAs[String]("partition") -> row.getAs[Long]("count"))
      .toMap

    val effectiveCounts: Seq[(String, Long, Long)] = updateCounts.map { case (partition, desired) =>
      val available = availableCounts.getOrElse(partition, 0L)
      require(
        available > 0,
        s"Round # $currentRound requests $desired updates for partition '$partition', " +
          "but no records exist in that partition in any prior round")
      if (available < desired) {
        println(
          s"WARN: Round # $currentRound requests $desired updates for partition '$partition' " +
            s"but only $available distinct keys exist; updating all $available")
      }
      (partition, Math.min(desired, available), available)
    }

    // Stratified over-sample so the exact row_number trim below sorts a small candidate set
    // instead of every key in the partition. The 1.2x + 100 margin makes an undershoot (sample
    // smaller than the desired count) vanishingly unlikely; the trim then restores exactness.
    val sampleFractions: Map[String, Double] = effectiveCounts.map {
      case (partition, desired, available) =>
        val fraction =
          if (desired >= available) 1.0
          else Math.min(1.0, (desired * 1.2 + 100.0) / available)
        partition -> fraction
    }.toMap
    val sampledDF = rankedDF.stat.sampleBy("partition", sampleFractions, SEED + currentRound)

    val desiredDF = effectiveCounts
      .map { case (partition, desired, _) => (partition, desired) }
      .toDF("partition", "desired_updates")
    val perPartitionWindow = Window.partitionBy("partition").orderBy(rand(SEED + currentRound))
    val pickedDF = sampledDF
      .withColumn("rn", row_number().over(perPartitionWindow))
      .join(broadcast(desiredDF), "partition")
      .filter($"rn" <= $"desired_updates")
      .drop("rn", "desired_updates")
    pickedDF.persist()

    val pickedCounts: Map[String, Long] = pickedDF
      .groupBy("partition")
      .count()
      .collect()
      .map(row => row.getAs[String]("partition") -> row.getAs[Long]("count"))
      .toMap
    effectiveCounts.foreach { case (partition, desired, _) =>
      val actual = pickedCounts.getOrElse(partition, 0L)
      if (actual != desired) {
        println(
          s"WARN: Round # $currentRound picked $actual update keys for partition '$partition' " +
            s"instead of the requested $desired (over-sample undershoot)")
      }
    }

    // Fetch the full-width rows only for the picked keys.
    (sourceDf.join(pickedDF, Seq("key", "partition", "round"), "inner"), Seq(rankedDF, pickedDF))
  }

  /**
   * When a custom Avro schema is supplied, the user-provided --record-size is just a width hint
   * for variable-length fields (strings/binary). The actual on-disk row size depends on the schema
   * and can be very different. Sample-write a bounded batch of rows as parquet, measure the compressed size,
   * and use that directly as bytes/record for parallelism. Since the sample is already compressed
   * parquet (same format as the real output), we skip COMPRESSION_RATIO_GUESS for this path.
   */
  private def resolveEffectiveSizing(
      path: String,
      schema: StructType,
      partitionPaths: List[String],
      recordSize: Int,
      keyType: KeyType,
      avroSchemaPath: Option[String]): (Int, Double) = {
    avroSchemaPath match {
      case Some(_) =>
        val estimated = estimateRecordSize(path, schema, partitionPaths, recordSize, keyType)
        println(s"""
             |$lineSepBold
             |Estimated record size from custom schema: $estimated bytes/record (compressed parquet avg over sample rows).
             |Overriding --record-size=$recordSize for parallelism computation.
             |$lineSepBold
             |""".stripMargin)
        (estimated, 1.0)
      case None => (recordSize, COMPRESSION_RATIO_GUESS)
    }
  }

  /**
   * Estimate the on-disk size per record for a given schema by writing a bounded number of
   * sample rows (~20MB worth, deduced from the --record-size hint) as a single parquet file
   * under `basePath` and measuring its size. The sample is generated
   * with the same ComplexDataGenerator code path used by the real workload so the estimate
   * reflects actual generator output (variable-length strings, nested types, nulls, etc.).
   *
   * The temp directory is always deleted before returning.
   *
   * @param basePath          parent path used to host the temp sample directory
   * @param schema            target schema (custom Avro or default)
   * @param partitionPaths    partition path values, used by the generator
   * @param recordSizeHint    user-supplied --record-size, used as a width hint for variable fields
   * @param keyType           key generation strategy
   * @return average bytes/record measured from the sample
   */
  private def estimateRecordSize(
      basePath: String,
      schema: StructType,
      partitionPaths: List[String],
      recordSizeHint: Int,
      keyType: KeyType): Int = {
    // Bound the sample by bytes, not a fixed row count: at large --record-size a fixed 100K-row
    // sample can dwarf the actual workload (100K x 100KB = ~10GB of throwaway data). Deduce the
    // row count from the user's --record-size to target ~20MB of sample data, clamped to
    // [1000, 200000] rows so tiny hints stay bounded and huge hints still sample enough rows
    // for a stable average.
    val targetSampleBytes = 20L * 1024 * 1024
    val sampleParallelism = 4
    val requestedSampleRows =
      Math.min(200000L, Math.max(1000L, targetSampleBytes / Math.max(recordSizeHint, 1)))
    // genParallelRDD generates floor(count / parallelism) rows per task, so keep the row count
    // an exact multiple of the parallelism — otherwise the bytes/record division below would
    // use more rows than were actually written.
    val sampleCount: Long = (requestedSampleRows / sampleParallelism) * sampleParallelism
    // Place the sample under the user's output path so the temp dir lives on the same filesystem
    // (avoids cross-FS issues when basePath is s3://, gs://, etc.).
    val samplePath = s"$basePath/.record_size_sample_${System.currentTimeMillis()}"
    val samplePathHadoop = new Path(samplePath)
    val fs = samplePathHadoop.getFileSystem(spark.sparkContext.hadoopConfiguration)

    // Single-bucket CDF: every sample row lands on partitionPaths.head. All partition values are
    // same-length date strings (YYYY-MM-DD), so the chosen partition doesn't affect record size.
    val sampleCDF = List(1.0)
    val samplePartitionPaths = List(partitionPaths.head)

    try {
      // Reuse generateNewRecord with the same per-task seeded RNG scheme as the real workload:
      // each task advances its own distinctly-seeded Random across its rows, so no two tasks
      // emit duplicate values that parquet dictionary encoding would dedupe (which would deflate
      // the measured bytes/record). Use a round number real rounds never use so the sample data
      // does not replicate round 0's exact values.
      val sampleRDD = genParallelRDD(spark, sampleParallelism, 0, sampleCount)
        .mapPartitionsWithIndex { (partIdx, it) =>
          val random = taskRandom(round = -1, partIdx)
          it.map(_ =>
            generateNewRecord(
              round = 0,
              size = recordSizeHint,
              partitionPaths = samplePartitionPaths,
              partitionDistributionCDF = sampleCDF,
              keyType = keyType,
              schema = schema,
              random = random))
        }

      val sampleDF = spark.createDataFrame(sampleRDD, schema)
      // repartition(1) writes a single parquet file so footer/dictionary overhead is measured
      // exactly once; generation upstream of the shuffle boundary still runs in parallel.
      sampleDF
        .repartition(1)
        .write
        .format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
        .mode(SaveMode.Overwrite)
        .save(samplePath)

      val totalBytes = fs.getContentSummary(samplePathHadoop).getLength
      println(s"Record size sample: $sampleCount rows, $totalBytes bytes on disk")
      Math.max((totalBytes / sampleCount).toInt, 1)
    } finally {
      if (fs.exists(samplePathHadoop)) {
        fs.delete(samplePathHadoop, true)
      }
    }
  }

  ////////////////////////////////////////
  // Generating updates based on distribution type.
  ////////////////////////////////////////
  private def generateUpdates(
      updatePatterns: UpdatePatterns,
      partitionPaths: List[String],
      numUpdateRecords: Long,
      numPartitionsToUpdate: Int,
      path: String,
      targetParallelism: Int,
      currentRound: Int,
      zipfianShape: Double): DataFrame = {
    val rawUpdatesDF = updatePatterns match {
      case Uniform =>
        getRandomlyDistributedUpdates(
          partitionPaths,
          numUpdateRecords,
          numPartitionsToUpdate,
          path,
          currentRound)
      case Zipf =>
        getZipfDistributedUpdates(
          partitionPaths,
          numUpdateRecords,
          numPartitionsToUpdate,
          path,
          currentRound,
          zipfianShape)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported update pattern: $updatePatterns")
    }

    val finalUpdatedDf = regenerateUpdateValues(rawUpdatesDF, currentRound)

    // NOTE: Applying this limit does not guarantee that exactly N elements will be contained in the
    //       returned dataset, since it might not be applying Spark's [[GlobalLimit]] operator.
    //       Instead, it might return slightly higher number of the records (but no more than O(number of partitions)),
    //       since we're simply applying [[LocalLimit]] to circumvent the performance implications of
    //       [[GlobalLimit]] for very large datasets (coalescing all partitions into a single one, then doing
    //       a limit on it)
    partitionLocalLimit(finalUpdatedDf.repartition(targetParallelism), numUpdateRecords.toInt)
  }

  /**
   * Regenerate all non-key scalar columns of the picked update rows with new values so updates
   * carry different data. Complex types (StructType, ArrayType, MapType) are left unchanged —
   * sufficient for benchmarking. Builds a single select() instead of chaining withColumn per
   * field: each withColumn adds another projection layer to the plan, and for wide schemas the
   * analyzer cost of the resulting deeply-nested plan dominates the actual work.
   */
  private def regenerateUpdateValues(
      rawUpdatesDF: DataFrame,
      currentRound: Int,
      nullifiedPartitions: Set[String] = Set.empty): DataFrame = {
    val newTs = System.currentTimeMillis()
    val updateSchema = rawUpdatesDF.schema
    val nullifiedCol =
      if (nullifiedPartitions.isEmpty) None
      else Some(col("partition").isin(nullifiedPartitions.toSeq: _*))
    def maybeNullify(field: StructField, real: Column): Column =
      nullifiedCol match {
        case Some(isNullified) => when(isNullified, lit(null).cast(field.dataType)).otherwise(real)
        case None => real
      }
    val projectedColumns = updateSchema.fields.map { field =>
      val column = field.name match {
        case "key" | "partition" => col(field.name)
        case "round" => lit(currentRound)
        case "ts" => lit(newTs)
        case _ =>
          val real = field.dataType match {
            case StringType => expr("uuid()")
            case LongType => (rand() * Long.MaxValue).cast(LongType)
            case IntegerType => (rand() * Int.MaxValue).cast(IntegerType)
            case FloatType => rand().cast(FloatType)
            case DoubleType => rand().cast(DoubleType)
            case BooleanType => (rand() > 0.5).cast(BooleanType)
            case DateType => current_date()
            case TimestampType => current_timestamp()
            case dt: DecimalType =>
              (rand() * Math.pow(10, dt.precision - dt.scale)).cast(dt)
            case _ =>
              col(
                field.name
              ) // leave complex types (StructType, ArrayType, MapType, BinaryType) unchanged
          }
          maybeNullify(field, real)
      }
      column.as(field.name)
    }
    rawUpdatesDF.select(projectedColumns: _*)
  }

  private def getZipfDistributedUpdates(
      partitionPaths: List[String],
      numUpdateRecords: Long,
      numPartitionsToWrite: Int,
      path: String,
      currentRound: Int,
      zipfianShape: Double): DataFrame = {
    val numRecordsPerPartition: List[Int] =
      MathUtils.zipfDistribution(numUpdateRecords, numPartitionsToWrite, shape = zipfianShape)
    val partitionsToUpdate = partitionPaths.take(numPartitionsToWrite)
    println(
      s"Generating zipf distributed updates from partitions for round # $currentRound: Partitions $partitionsToUpdate")

    var sourceDf = spark.read.format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT).load(s"$path/*")
    sourceDf = sourceDf.filter(col("partition").isin(partitionsToUpdate: _*))
    sourceDf.createOrReplaceTempView("source_df_partitions")

    var rankedDF = spark.sql("""
        | SELECT key, partition, `round`, rank(key) OVER (PARTITION BY key ORDER BY round DESC) as key_rank
        | FROM source_df_partitions
        |""".stripMargin)
    rankedDF = rankedDF.filter($"key_rank" === 1).drop(s"key_rank")
    rankedDF.persist()

    val partitionCounts: Map[String, Long] = rankedDF
      .groupBy("partition")
      .count()
      .collect()
      .map(row => row.getAs[String]("partition") -> row.getAs[Long]("count"))
      .toMap

    val samplingRatios: Map[String, Double] = partitionCounts.map {
      case (partition, totalRecords) =>
        val desiredCount = numRecordsPerPartition(partitionPaths.indexOf(partition))
        val ratio = Math.min(1.0, desiredCount.toDouble / totalRecords.toDouble)
        partition -> ratio
    }

    var fullPlan = spark.emptyDataFrame
    var count: Int = 0
    samplingRatios.foreach(x => {
      val ppf = rankedDF.filter($"partition" === x._1).sample(x._2)
      ppf.persist()
      fullPlan = if (count == 0) {
        count = 1
        ppf
      } else {
        fullPlan.union(ppf)
      }
    })

    // Join sourceDf with fullPlan on key, partition, and round
    val joinCols = Seq("key", "partition", "round")
    val joinedDf = sourceDf.join(fullPlan, joinCols, "inner")
    joinedDf
  }

  private def getRandomlyDistributedUpdates(
      partitionPaths: List[String],
      numUpdateRecords: Long,
      numPartitionsToWrite: Int,
      path: String,
      currentRound: Int): DataFrame = {
    val partitionsToUpdate = partitionPaths.take(numPartitionsToWrite)
    println(
      s"Generating random updates from partitions for round # $currentRound: Partitions $partitionsToUpdate")

    var sourceDf = spark.read.format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT).load(s"$path/*")
    sourceDf = sourceDf.filter(col("partition").isin(partitionsToUpdate: _*))
    sourceDf.select("key", "partition", "round").createOrReplaceTempView("source_df_partitions")

    // Rank on the narrow (key, partition, round) projection only — running the window over
    // SELECT * shuffles every column of the source through the rank, which is prohibitively
    // expensive for wide schemas. The sampled keys are joined back to the full rows below
    // (same structure as the Zipf path).
    var rankedDF = spark.sql("""
        | SELECT key, partition, `round`, rank(key) OVER (PARTITION BY key ORDER BY round DESC) as key_rank
        | FROM source_df_partitions
        |""".stripMargin)
    rankedDF = rankedDF.filter($"key_rank" === 1).drop(s"key_rank")
    rankedDF.persist()
    val totalRecords = rankedDF.count()
    // Oversample by 10% to compensate for Spark's probabilistic sampling, then limit to exact count
    val samplingRatio = Math.min(1.0, numUpdateRecords.toDouble / totalRecords.toDouble * 1.1)
    println(
      s"Picking random updates for round: # $currentRound: from total records = $totalRecords, " +
        s"targeted update records = $numUpdateRecords, sampling ratio = $samplingRatio")

    val sampledKeys = rankedDF
      .sample(samplingRatio)
      .limit(numUpdateRecords.toInt)

    // Fetch the full-width rows only for the sampled keys.
    sourceDf.join(sampledKeys, Seq("key", "partition", "round"), "inner")
  }

  private def genDateBasedPartitionValues(targetPartitionsCount: Int): List[String] = {
    // This will generate an ordered sequence of dates in the format of "yyyy/mm/dd"
    // (where most recent one is the first element)
    List
      .fill(targetPartitionsCount)(LocalDate.now())
      .zipWithIndex
      .map(t => t._1.minusDays(targetPartitionsCount - t._2))
      .map(d => s"${d.getYear}-${"%02d".format(d.getMonthValue)}-${"%02d".format(d.getDayOfMonth)}")
      .reverse
  }

  private def genPartitionsDistributionMatrix(
      totalPartitions: Int,
      partitionDistributionMatrixOpt: Option[List[List[Double]]]) =
    ChangeDataGenerator.genPartitionsDistributionMatrix(
      totalPartitions,
      partitionDistributionMatrixOpt,
      numRounds)
}

object ChangeDataGenerator {
  val RECORD_KEY_FIELD_NAME = "key"
  val PARTITION_PATH_FIELD_NAME = "partition"
  val COMPRESSION_RATIO_GUESS = .66
  val DEFAULT_DATA_GEN_FORMAT: String = "parquet"

  /**
   * Validate and expand `partitionDistributionMatrixOpt` into the matrix consumed by the
   * generator. When the option is `None`, falls back to a uniform `1.0 / totalPartitions` row
   * replicated for every round.
   */
  private[lakeloader] def genPartitionsDistributionMatrix(
      totalPartitions: Int,
      partitionDistributionMatrixOpt: Option[List[List[Double]]],
      numRounds: Int): (Int, List[List[Double]]) =
    partitionDistributionMatrixOpt match {
      case Some(partitionDistMatrix) =>
        assert(partitionDistMatrix.size == numRounds)
        partitionDistMatrix.foreach { dist =>
          assert(
            totalPartitions == -1 || totalPartitions == dist.size,
            s"$totalPartitions != ${dist.size}")
          assert(
            math.abs(dist.sum - 1.0) < 1e-5,
            s"partition distribution row weights must sum to 1.0, got ${dist.sum}")
        }
        (partitionDistMatrix.head.size, partitionDistMatrix)

      case None =>
        val dist = List.fill(totalPartitions)(1.0 / totalPartitions)
        (dist.size, List.fill(numRounds)(dist))
    }

  /**
   * Expand the CLI `--partition-distribution` spec into the per-round insert-weight matrix
   * consumed by [[ChangeDataGenerator.generateWorkload]].
   *
   * Each segment is the leading non-zero weights for that batch; the rest is zero-padded up to
   * `totalPartitions`. A `None` segment means "uniform across totalPartitions" for that batch.
   * Round 0 uses `spec.firstRound`; rounds 1..numRounds-1 use `spec.subsequentRounds`.
   *
   * Returns `None` when the user did not pass the flag — caller falls through to the existing
   * uniform-matrix default in `genPartitionsDistributionMatrix`.
   */
  private[lakeloader] def buildPartitionDistributionMatrix(
      specOpt: Option[PartitionDistributionSpec],
      totalPartitions: Int,
      numRounds: Int): Option[List[List[Double]]] = {
    specOpt.map { spec =>
      require(
        totalPartitions > 0,
        "--total-partitions must be set when using --partition-distribution")
      def buildRow(leading: Option[List[Double]]): List[Double] = leading match {
        case None => List.fill(totalPartitions)(1.0 / totalPartitions)
        case Some(weights) =>
          require(
            weights.size <= totalPartitions,
            s"--partition-distribution segment has ${weights.size} entries, exceeds --total-partitions=$totalPartitions")
          weights ++ List.fill(totalPartitions - weights.size)(0.0)
      }
      val firstRow = buildRow(spec.firstRound)
      val subsequentRow = buildRow(spec.subsequentRounds)
      if (numRounds <= 1) List.fill(numRounds)(firstRow)
      else firstRow :: List.fill(numRounds - 1)(subsequentRow)
    }
  }

  def main(args: Array[String]): Unit = {

    ChangeDataGeneratorParser.parser.parse(args, DatagenConfig()) match {
      case Some(config) =>
        val spark = SparkSession.builder
          .appName("ChangeDataGeneratorApp")
          .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
          .getOrCreate()

        config.workloadSpecPath match {
          case Some(specPath) =>
            val spec =
              FineGrainedWorkloadSpec.fromJsonFile(specPath, spark.sparkContext.hadoopConfiguration)
            val bootstrapDescription = spec.bootstrap match {
              case Some(bootstrap) =>
                s"${bootstrap.numPartitions} bootstrap partitions [${bootstrap.startDate} .. ${bootstrap.endDate}]"
              case None =>
                s"external bootstrap (${spec.externalBootstrap.get.tablePath}), round 0 skipped"
            }
            println(
              s"Using fine-grained workload spec from $specPath: " +
                s"${spec.totalRounds - spec.startRound} rounds starting at round ${spec.startRound} " +
                s"(${spec.commits.size} commits), $bootstrapDescription. " +
                "Flags --number-rounds, --number-records-per-round, --update-ratio, " +
                "--total-partitions, --partition-distribution, --update-pattern and " +
                "--num-partitions-to-update are ignored in this mode.")
            val changeDataGenerator = new ChangeDataGenerator(spark, spec.totalRounds)
            changeDataGenerator.generateFineGrainedWorkload(
              config.outputPath,
              spec,
              numColumns = config.numberColumns,
              recordSize = config.recordSize,
              targetDataFileSize = config.targetDataFileSize,
              skipIfExists = config.skipIfExists,
              keyType = config.keyType,
              // config.startRound defaults to 0 whether or not the user passed --start-round;
              // treat the default as "unset" so externalBootstrap specs (whose valid start round
              // is 1, not 0) get the right default without requiring --start-round 1 on every
              // invocation. An explicit non-zero --start-round always passes through as-is.
              startRound = if (config.startRound == 0) -1 else config.startRound,
              avroSchemaPath = config.avroSchemaPath)

          case None =>
            val partitionDistributionMatrixOpt = buildPartitionDistributionMatrix(
              config.partitionDistribution,
              config.totalPartitions,
              config.numberOfRounds)

            val changeDataGenerator = new ChangeDataGenerator(spark, config.numberOfRounds)
            changeDataGenerator.generateWorkload(
              config.outputPath,
              roundsDistribution = {
                val dist = config.roundsDistribution
                if (dist.size >= config.numberOfRounds) dist.take(config.numberOfRounds)
                else dist ++ List.fill(config.numberOfRounds - dist.size)(dist.last)
              },
              numColumns = config.numberColumns,
              recordSize = config.recordSize,
              updateRatio = config.updateRatio,
              totalPartitions = config.totalPartitions,
              partitionDistributionMatrixOpt = partitionDistributionMatrixOpt,
              targetDataFileSize = config.targetDataFileSize,
              skipIfExists = config.skipIfExists,
              keyType = config.keyType,
              startRound = config.startRound,
              updatePatterns = config.updatePattern,
              numPartitionsToUpdate = config.numPartitionsToUpdate,
              zipfianShape = config.zipfianShape,
              avroSchemaPath = config.avroSchemaPath)
        }

        spark.stop()

      case None =>
        // scopt already prints help
        sys.exit(1)
    }
  }

  private def genParallelRDD(
      spark: SparkSession,
      targetParallelism: Int,
      start: Long,
      end: Long): RDD[Long] = {
    val partitionSize = (end - start) / targetParallelism
    spark.sparkContext
      .parallelize(0 until targetParallelism, targetParallelism)
      .mapPartitions { it =>
        val partitionStart = it.next() * partitionSize
        (partitionStart until partitionStart + partitionSize).iterator
      }
  }

  /**
   * Like [[genParallelRDD]] but generates *exactly* `count` global indices [0, count): the
   * remainder `count % targetParallelism` is spread one-per-task instead of dropped. Used by the
   * fine-grained spec path where record counts must match the spec exactly.
   */
  private def genExactParallelRDD(
      spark: SparkSession,
      targetParallelism: Int,
      count: Long): RDD[Long] = {
    val base = count / targetParallelism
    val remainder = count % targetParallelism
    spark.sparkContext
      .parallelize(0 until targetParallelism, targetParallelism)
      .mapPartitions { it =>
        val taskIdx = it.next()
        val start = taskIdx * base + Math.min(taskIdx.toLong, remainder)
        val taskCount = base + (if (taskIdx < remainder) 1 else 0)
        (start until start + taskCount).iterator
      }
  }
}
