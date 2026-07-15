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

import ai.onehouse.lakeloader.ChangeDataGenerator.{genParallelRDD, COMPRESSION_RATIO_GUESS, PARTITION_PATH_FIELD_NAME, RECORD_KEY_FIELD_NAME}
import ai.onehouse.lakeloader.configs.{DatagenConfig, KeyTypes, PartitionDistributionSpec, UpdatePatterns}
import ai.onehouse.lakeloader.configs.KeyTypes.KeyType
import ai.onehouse.lakeloader.configs.UpdatePatterns.{Uniform, UpdatePatterns, Zipf}
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser
import ai.onehouse.lakeloader.utils.{AvroSchemaUtils, ComplexDataGenerator, MathUtils, StringUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CatalystUtil.partitionLocalLimit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
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
  private val random = new Random(SEED)

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
      schema: StructType) = {
    ComplexDataGenerator.generateRow(
      schema,
      round,
      partitionPaths,
      partitionDistributionCDF,
      keyType,
      size,
      random)
  }

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
      updateRatios: List[Double] = List(0.5),
      totalPartitions: Int = -1,
      partitionDistributionMatrixOpt: Option[List[List[Double]]] = None,
      targetDataFileSize: Int = 128 * 1024 * 1024,
      skipIfExists: Boolean = false,
      keyType: KeyType = KeyTypes.Random,
      startRound: Int = 0,
      updatePatterns: List[UpdatePatterns] = List(UpdatePatterns.Uniform),
      numPartitionsToUpdate: List[Int] = List(-1),
      zipfianShapes: List[Double] = List(2.93),
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
    require(updateRatios.nonEmpty, "updateRatios must not be empty")
    require(updatePatterns.nonEmpty, "updatePatterns must not be empty")
    require(numPartitionsToUpdate.nonEmpty, "numPartitionsToUpdate must not be empty")
    require(zipfianShapes.nonEmpty, "zipfianShapes must not be empty")
    // `-1` is the documented sentinel for "no partition-count constraint";
    // any non-negative value must not exceed totalPartitions.
    require(
      numPartitionsToUpdate.forall(n => n == -1 || n <= totalPartitions),
      s"Each numPartitionsToUpdate entry must be -1 (unbounded) or <= totalPartitions=$totalPartitions; got $numPartitionsToUpdate")

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

    // When a custom Avro schema is supplied, the user-provided --record-size is just a width hint
    // for variable-length fields (strings/binary). The actual on-disk row size depends on the schema
    // and can be very different. Sample-write 100K rows as parquet, measure the compressed size,
    // and use that directly as bytes/record for parallelism. Since the sample is already compressed
    // parquet (same format as the real output), we skip COMPRESSION_RATIO_GUESS for this path.
    val (effectiveRecordSize, effectiveCompressionRatio) = avroSchemaPath match {
      case Some(_) =>
        val estimated = estimateRecordSize(
          path,
          schema,
          partitionPaths,
          recordSize,
          keyType)
        println(
          s"""
             |$lineSepBold
             |Estimated record size from custom schema: $estimated bytes/record (compressed parquet avg over 100K sample rows).
             |Overriding --record-size=$recordSize for parallelism computation.
             |$lineSepBold
             |""".stripMargin)
        (estimated, 1.0)
      case None => (recordSize, COMPRESSION_RATIO_GUESS)
    }

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
        // Per-round parameter lookup — last-value-fill semantics so a scalar can broadcast.
        val roundUpdateRatio = ChangeDataGenerator.valueForRound(updateRatios, curRound)
        val roundUpdatePattern = ChangeDataGenerator.valueForRound(updatePatterns, curRound)
        val roundNumPartitionsToUpdate = ChangeDataGenerator.valueForRound(numPartitionsToUpdate, curRound)
        val roundZipfianShape = ChangeDataGenerator.valueForRound(zipfianShapes, curRound)

        // Calculate inserts/updates split
        val targetRecords = roundsDistribution(curRound)
        val numUpdates =
          if (curRound == 0 || roundNumPartitionsToUpdate <= 0) 0
          else Math.min((roundUpdateRatio * targetRecords).toLong, curRound * targetRecords)
        val numInserts = targetRecords - numUpdates

        // Use ceiling so the per-file size never exceeds targetDataFileSize. With floor,
        // 7.67 truncates to 7 and each file overshoots the cap; ceil → 8 keeps every file
        // strictly under the configured target (default 128 MB).
        val estimatedTotalBytes =
          targetRecords.toDouble * effectiveRecordSize * effectiveCompressionRatio
        val targetParallelism = Math.max(
          2,
          Math.ceil(estimatedTotalBytes / targetDataFileSize).toInt)

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
          .map(_ =>
            generateNewRecord(
              curRound,
              recordSize,
              partitionPaths,
              partitionDistributionCDF,
              keyType,
              schema))

        val insertsDF = spark.createDataFrame(insertsRDD, schema)
        val upsertDF =
          if (numUpdates == 0) insertsDF
          else
            insertsDF.union(
              generateUpdates(
                roundUpdatePattern,
                partitionPaths,
                numUpdates,
                roundNumPartitionsToUpdate,
                path,
                targetParallelism,
                curRound,
                roundZipfianShape))

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
   * Estimate the on-disk size per record for a given schema by writing 100K sample rows to a
   * temporary parquet directory under `basePath` and measuring its size. The sample is generated
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
    val sampleCount: Long = 100000L
    val sampleParallelism = 4
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
      // Reuse generateNewRecord so the sample uses the same closure-captured `this.random` per
      // task as the real workload — each task advances one Random across its rows, mirroring
      // production-path entropy. Constructing `new Random(SEED)` per row instead would make every
      // sample row identical and compress unrealistically well.
      val sampleRDD = genParallelRDD(spark, sampleParallelism, 0, sampleCount)
        .map(_ =>
          generateNewRecord(
            round = 0,
            size = recordSizeHint,
            partitionPaths = samplePartitionPaths,
            partitionDistributionCDF = sampleCDF,
            keyType = keyType,
            schema = schema))

      val sampleDF = spark.createDataFrame(sampleRDD, schema)
      sampleDF
        .repartition(1)
        .write
        .format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
        .mode(SaveMode.Overwrite)
        .save(samplePath)

      val totalBytes = fs.getContentSummary(samplePathHadoop).getLength
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

    val newTs = System.currentTimeMillis()
    val updateSchema = rawUpdatesDF.schema
    // Regenerate all non-key scalar columns with new values so updates have different data.
    // Complex types (StructType, ArrayType, MapType) are left unchanged — sufficient for benchmarking.
    val finalUpdatedDf = updateSchema.fields.foldLeft(rawUpdatesDF) { (df, field) =>
      field.name match {
        case "key" | "partition" => df
        case "round" => df.withColumn(field.name, lit(currentRound))
        case "ts" => df.withColumn(field.name, lit(newTs))
        case _ =>
          field.dataType match {
            case StringType => df.withColumn(field.name, expr("uuid()"))
            case LongType => df.withColumn(field.name, (rand() * Long.MaxValue).cast(LongType))
            case IntegerType => df.withColumn(field.name, (rand() * Int.MaxValue).cast(IntegerType))
            case FloatType => df.withColumn(field.name, rand().cast(FloatType))
            case DoubleType => df.withColumn(field.name, rand().cast(DoubleType))
            case BooleanType => df.withColumn(field.name, (rand() > 0.5).cast(BooleanType))
            case DateType => df.withColumn(field.name, current_date())
            case TimestampType => df.withColumn(field.name, current_timestamp())
            case dt: DecimalType =>
              df.withColumn(field.name, (rand() * Math.pow(10, dt.precision - dt.scale)).cast(dt))
            case _ => df // leave complex types (StructType, ArrayType, MapType, BinaryType) unchanged
          }
      }
    }

    // NOTE: Applying this limit does not guarantee that exactly N elements will be contained in the
    //       returned dataset, since it might not be applying Spark's [[GlobalLimit]] operator.
    //       Instead, it might return slightly higher number of the records (but no more than O(number of partitions)),
    //       since we're simply applying [[LocalLimit]] to circumvent the performance implications of
    //       [[GlobalLimit]] for very large datasets (coalescing all partitions into a single one, then doing
    //       a limit on it)
    partitionLocalLimit(finalUpdatedDf.repartition(targetParallelism), numUpdateRecords.toInt)
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
    sourceDf.createOrReplaceTempView("source_df_partitions")

    var rankedDF = spark.sql("""
        | SELECT *, rank(key) OVER (PARTITION BY key ORDER BY round DESC) as key_rank
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

    val finalDF = rankedDF
      .sample(samplingRatio)
      .limit(numUpdateRecords.toInt)
    finalDF
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
   * Look up the per-round value in `list`. Uses last-value-fill: if `round` is
   * beyond the list length, returns `list.last`. A scalar (single-entry list)
   * broadcasts to every round; a shorter list holds its final value for the
   * remaining rounds; a longer list is truncated by the caller before this
   * helper is invoked. Callers must ensure `list` is non-empty.
   */
  private[lakeloader] def valueForRound[T](list: List[T], round: Int): T = {
    require(list.nonEmpty, "value list must not be empty")
    if (round < list.size) list(round) else list.last
  }

  /**
   * Right-pad `list` to exactly `n` entries by repeating its last value; if
   * `list` already has more than `n` entries, truncate to `n`. Mirrors the
   * existing behavior for `--number-records-per-round`.
   */
  private[lakeloader] def padOrTruncate[T](list: List[T], n: Int): List[T] = {
    require(list.nonEmpty, "list must not be empty")
    if (list.size >= n) list.take(n)
    else list ++ List.fill(n - list.size)(list.last)
  }

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
        val partitionDistributionMatrixOpt = buildPartitionDistributionMatrix(
          config.partitionDistribution,
          config.totalPartitions,
          config.numberOfRounds)

        val changeDataGenerator = new ChangeDataGenerator(spark, config.numberOfRounds)
        changeDataGenerator.generateWorkload(
          config.outputPath,
          roundsDistribution = padOrTruncate(config.roundsDistribution, config.numberOfRounds),
          numColumns = config.numberColumns,
          recordSize = config.recordSize,
          updateRatios = padOrTruncate(config.updateRatios, config.numberOfRounds),
          totalPartitions = config.totalPartitions,
          partitionDistributionMatrixOpt = partitionDistributionMatrixOpt,
          targetDataFileSize = config.targetDataFileSize,
          skipIfExists = config.skipIfExists,
          keyType = config.keyType,
          startRound = config.startRound,
          updatePatterns = padOrTruncate(config.updatePatterns, config.numberOfRounds),
          numPartitionsToUpdate = padOrTruncate(config.numPartitionsToUpdate, config.numberOfRounds),
          zipfianShapes = padOrTruncate(config.zipfianShapes, config.numberOfRounds),
          avroSchemaPath = config.avroSchemaPath)

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
}
