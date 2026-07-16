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

import ai.onehouse.lakeloader.configs.{DatagenConfig, KeyTypes, SynthesizerConfig, UpdatePatterns}
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Path => JPath}
import java.util.UUID
import scala.io.Source

/**
 * End-to-end integration test. Writes a small Hudi table via Spark with a
 * known workload shape, runs the WorkloadSynthesizer against it, then parses
 * the emitted synth-full.flags through ChangeDataGeneratorParser and asserts
 * the derived values are within tolerance of ground truth.
 *
 * Reuses one SparkSession across scenarios to amortize the ~10s cold start.
 */
class WorkloadSynthesizerE2ESpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempRoot: JPath = _

  override def beforeAll(): Unit = {
    tempRoot = Files.createTempDirectory("wls-e2e-")
    spark = SparkSession.builder()
      .appName("WorkloadSynthesizerE2ESpec")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.referenceTracking", "false")
      .config("spark.kryo.registrationRequired", "false")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.userClassPathFirst", "true")
      .config("spark.executor.userClassPathFirst", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (tempRoot != null) deleteRecursively(tempRoot.toFile)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete()
  }

  /** Build a single batch with per-partition insert weights. */
  private def buildBatch(
      partitionWeights: Map[String, Long],
      keyPrefix: String,
      keysToUpdate: Seq[String] = Seq.empty): org.apache.spark.sql.DataFrame = {
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("partition", StringType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("value", IntegerType, nullable = true)))
    val now = System.currentTimeMillis()
    // Raw UUIDs (no per-commit prefix). Real customer UUIDs are unnamespaced,
    // and a per-commit "kN-" prefix produces artificial min-value monotonicity
    // across commits (Spearman correlation → 1.0) that misleads the temporal
    // signal in the classifier.
    val insertRows = partitionWeights.toSeq.flatMap { case (partition, count) =>
      (0L until count).map { i =>
        Row(UUID.randomUUID().toString, partition, now + i, i.toInt)
      }
    }
    val updateRows = keysToUpdate.map { k =>
      // Send the same key back with a bumped ts; partition path stays the same.
      // We look up the partition via a marker embedded in the update-key list.
      val parts = k.split("::", 2)
      Row(parts(0), parts(1), now + 999999L, 42)
    }
    spark.createDataFrame(spark.sparkContext.parallelize(insertRows ++ updateRows), schema)
  }

  private def writeHudi(
      df: org.apache.spark.sql.DataFrame,
      tablePath: String,
      tableName: String,
      mode: SaveMode): Unit = {
    df.write
      .format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "partition")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.operation", if (mode == SaveMode.Overwrite) "bulk_insert" else "upsert")
      .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
      .option("hoodie.table.type", "COPY_ON_WRITE")
      .option("hoodie.parquet.small.file.limit", "0")
      .option("hoodie.parquet.max.file.size", (32L * 1024L * 1024L).toString)
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option("hoodie.bulkinsert.shuffle.parallelism", "2")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .mode(mode)
      .save(tablePath)
  }

  private def parseEmittedFullFlags(outputDir: String): DatagenConfig = {
    val flagPath = new File(outputDir, "synth-full.flags")
    val raw = Source.fromFile(flagPath).mkString
    // Strip placeholder tokens and single-quote wrapping around --partition-distribution.
    val runnable = raw
      .replace("<fill-in>.avsc", "/tmp/dummy.avsc")
      .replace("<fill-in>", "/tmp/dummy-out")
    val args = runnable.trim.split("\\s+").map(_.replaceAll("^'|'$", ""))
    ChangeDataGeneratorParser.parser.parse(args, DatagenConfig())
      .getOrElse(fail(s"parser rejected synth-full.flags:\n$raw"))
  }

  test("E2E: uniform-inserts table with UUID keys → Random keys, low update ratio, Uniform pattern") {
    val tablePath = new File(tempRoot.toFile, "uniform_table").getAbsolutePath
    val outputDir = new File(tempRoot.toFile, "uniform_output").getAbsolutePath

    // Ground truth: 5 partitions, 3 commits. Commit 0: 500 inserts spread ~evenly across all 5.
    // Commits 1, 2: mixed inserts+updates, updates concentrated on partitions p0 and p1 only.
    val allPartitions = (0 until 5).map(i => s"p$i")

    val commit0Weights = allPartitions.map(p => p -> 100L).toMap
    writeHudi(buildBatch(commit0Weights, "k0"), tablePath, "uniform_e2e", SaveMode.Overwrite)

    // Fetch some existing keys from p0/p1 to use as updates in later commits.
    val existingKeys = spark.read.format("hudi").load(tablePath)
      .select("id", "partition")
      .where(col("partition").isin("p0", "p1"))
      .limit(60)
      .collect()
      .map(r => s"${r.getString(0)}::${r.getString(1)}")
    val commit1Updates = existingKeys.take(30).toSeq
    val commit2Updates = existingKeys.slice(30, 60).toSeq

    val commit1Weights = allPartitions.map(p => p -> 80L).toMap
    writeHudi(buildBatch(commit1Weights, "k1", commit1Updates), tablePath, "uniform_e2e", SaveMode.Append)

    val commit2Weights = allPartitions.map(p => p -> 80L).toMap
    writeHudi(buildBatch(commit2Weights, "k2", commit2Updates), tablePath, "uniform_e2e", SaveMode.Append)

    val cfg = SynthesizerConfig(tablePath = tablePath, outputDir = outputDir)
    WorkloadSynthesizer.run(spark, cfg)

    val parsed = parseEmittedFullFlags(outputDir)

    // 3 commits observed
    assert(parsed.numberOfRounds == 3, s"expected 3 rounds, got ${parsed.numberOfRounds}")
    // 5 distinct partitions
    assert(parsed.totalPartitions == 5, s"expected totalPartitions=5, got ${parsed.totalPartitions}")
    // Updates only in commits 1 and 2. Roughly (0 + 30/(30+400) + 30/(30+400)) / 3 ≈ 0.047 avg.
    // Give a generous window since Hudi reports may differ slightly.
    assert(parsed.updateRatio < 0.15, s"expected small update ratio, got ${parsed.updateRatio}")
    // Uniform inserts → Uniform pattern expected
    assert(parsed.updatePattern == UpdatePatterns.Uniform,
      s"expected Uniform pattern, got ${parsed.updatePattern}")
    // UUID-shaped keys → Random
    assert(parsed.keyType == KeyTypes.Random, s"expected Random key type, got ${parsed.keyType}")

    // Audit file exists and contains something recognizable
    val audit = new File(outputDir, "synth-audit.txt")
    assert(audit.exists() && audit.length() > 0)
    val auditText = Source.fromFile(audit).mkString
    assert(auditText.contains("source table: " + tablePath))
    assert(auditText.contains("record key field: id"))
  }

  test("E2E: highly-skewed inserts → Zipf pattern with fitted shape") {
    val tablePath = new File(tempRoot.toFile, "skewed_table").getAbsolutePath
    val outputDir = new File(tempRoot.toFile, "skewed_output").getAbsolutePath

    // 10 partitions. Insert counts follow 1/r^2 (shape=2), so p0 gets ~1000, p1 ~250, p2 ~110, ...
    // Two identical-shape commits so the fit has more signal.
    val zipfWeights: Map[String, Long] = (1 to 10).map { r =>
      s"p${r - 1}" -> math.max(1L, (2000.0 / math.pow(r, 2)).toLong)
    }.toMap

    writeHudi(buildBatch(zipfWeights, "s0"), tablePath, "skewed_e2e", SaveMode.Overwrite)
    writeHudi(buildBatch(zipfWeights, "s1"), tablePath, "skewed_e2e", SaveMode.Append)

    val cfg = SynthesizerConfig(tablePath = tablePath, outputDir = outputDir)
    WorkloadSynthesizer.run(spark, cfg)

    val parsed = parseEmittedFullFlags(outputDir)
    assert(parsed.numberOfRounds == 2)
    assert(parsed.totalPartitions == 10, s"expected 10 partitions, got ${parsed.totalPartitions}")
    assert(parsed.updatePattern == UpdatePatterns.Zipf,
      s"expected Zipf pattern on skewed inserts, got ${parsed.updatePattern}")
    // Fitted shape from ground-truth s=2. Hudi's writes shuffle records across
    // executors and some file-group placement noise creeps in; allow ±0.4.
    assert(math.abs(parsed.zipfianShape - 2.0) < 0.4,
      s"expected zipf shape ~2.0, got ${parsed.zipfianShape}")
    // Top partition should carry the largest share
    assert(parsed.partitionDistribution.isDefined)
    val partDist = ChangeDataGeneratorParser.parsePartitionDistribution(
      // Reconstruct the raw --partition-distribution string from the emitted config
      // by concatenating first+subsequent segments.
      parsed.partitionDistribution.get.firstRound.map(_.mkString(",")).getOrElse("") +
        parsed.partitionDistribution.get.subsequentRounds
          .map(w => ";" + w.mkString(","))
          .getOrElse(""))
    val leadingWeights = partDist.subsequentRounds.getOrElse(partDist.firstRound.getOrElse(Nil))
    assert(leadingWeights.head > 0.4,
      s"expected dominant leading weight > 0.4, got ${leadingWeights.head}")
  }
}
