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

import ai.onehouse.lakeloader.configs.FineGrainedWorkloadSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import scala.reflect.io.Directory

/**
 * End-to-end test for the `externalBootstrap` fine-grained workload path: builds a small local
 * Hudi table (standing in for a bootstrap done outside lake-loader), then generates incremental
 * rounds against it and asserts: round 0 is never generated, every round's record count matches
 * the spec exactly, new inserts get fresh keys, and update rows reuse real keys read back from
 * the external table.
 *
 * Inserts/updates are distinguished the same way as [[FineGrainedWorkloadGenTest]]: the round
 * suffix embedded in a key ("uuid-%03d" of the round it was created in) reveals whether a row's
 * key was freshly minted this round (insert) or reused from an earlier round -- here, always
 * round 0, since round 0's keys are exactly the bootstrap table's real keys.
 *
 * NOTE: as of this writing, running this suite under `mvn test` fails at the Hudi write step
 * with `ClassNotFoundException: scala.math.Ordering$Reverse` -- a pre-existing mismatch between
 * this repo's pinned `scala.version` (2.12.10) and the scala-library version the Hudi Spark
 * bundle's Kryo registration expects (unrelated to this feature; the same mismatch would affect
 * any test exercising a real Hudi write, and none existed before this one). The equivalent
 * end-to-end flow was independently verified via local `spark-submit` (see
 * project_lake_loader_external_bootstrap memory / PR description for the transcript). Fixing the
 * pom-wide scala-library pin is out of scope here.
 */
class ExternalBootstrapWorkloadGenTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var workDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    // Hudi mandates Kryo. Explicitly set the context classloader to this test class's loader
    // first -- under scalatest-maven-plugin's forked JVM, Kryo's AllScalaRegistrar resolves
    // built-in scala classes (e.g. scala.math.Ordering$Reverse) via
    // Thread.currentThread().getContextClassLoader(), which the plugin does not otherwise point
    // at the full test classpath.
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)
    workDir = Files.createTempDirectory("external_bootstrap_workload_test")
    spark = SparkSession.builder
      .master("local[2]")
      .appName("ExternalBootstrapWorkloadGenTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (workDir != null) {
      new Directory(workDir.toFile).deleteRecursively()
    }
  }

  private def keyRoundSuffix(df: DataFrame): DataFrame =
    df.withColumn("key_round", expr("cast(substring(key, -3, 3) as int)"))

  test("externalBootstrap: round 0 is skipped, inserts are fresh, updates reuse real Hudi keys") {
    // Step 1: build a small "external" Hudi table using the normal (non-external) fine-grained
    // path plus a plain bulk_insert write -- standing in for a bootstrap done outside lake-loader.
    val bootstrapSpec = FineGrainedWorkloadSpec.fromJsonString("""
      |{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-03", "totalRecords": 30}}
      |""".stripMargin)
    val rawInputPath = s"file://${workDir.toAbsolutePath}/raw_input"
    new ChangeDataGenerator(spark, bootstrapSpec.totalRounds)
      .generateFineGrainedWorkload(rawInputPath, bootstrapSpec, recordSize = 256)

    val tablePath = s"file://${workDir.toAbsolutePath}/hudi_table"
    spark.read
      .parquet(s"$rawInputPath/0")
      .write
      .format("hudi")
      .option("hoodie.table.name", "external_bootstrap_test")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "key")
      .option("hoodie.datasource.write.partitionpath.field", "partition")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    val bootstrapTableDF = spark.read.format("hudi").load(tablePath)
    assert(bootstrapTableDF.count() == 30)

    // Step 2: externalBootstrap-mode spec pointing at that table. Both update-targeted
    // partitions (2026-01-01, 2026-01-03) were part of the original bootstrap, per the
    // externalBootstrap constraint documented on ExternalBootstrapSpec.
    val incSpec = FineGrainedWorkloadSpec.fromJsonString(s"""
      |{
      |  "externalBootstrap": {"tablePath": "$tablePath"},
      |  "commits": [
      |    {"2026-01-01": {"inserts": 5, "updates": 4}, "2026-01-02": {"inserts": 3}},
      |    {"2026-01-03": {"inserts": 2, "updates": 3}}
      |  ]
      |}""".stripMargin)
    assert(incSpec.startRound == 1)
    assert(incSpec.bootstrap.isEmpty)

    val incOutputPath = s"file://${workDir.toAbsolutePath}/inc_output"
    new ChangeDataGenerator(spark, incSpec.totalRounds)
      .generateFineGrainedWorkload(incOutputPath, incSpec, recordSize = 256)

    // Round 0 must never be generated in externalBootstrap mode.
    val round0Path = new Path(s"$incOutputPath/0")
    val fs = round0Path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    assert(!fs.exists(round0Path), "round 0 must not be generated in externalBootstrap mode")

    // Round 1: 5 inserts + 4 updates into 2026-01-01, 3 inserts opening 2026-01-02.
    val round1 = keyRoundSuffix(spark.read.parquet(s"$incOutputPath/1"))
    assert(round1.count() == 12)
    assert(round1.filter(col("round") =!= 1).count() == 0)

    val round1p1 = round1.filter(col("partition") === "2026-01-01")
    assert(round1p1.count() == 9)
    assert(round1p1.filter(col("key_round") === 1).count() == 5) // fresh inserts
    val round1p1Updates = round1p1.filter(col("key_round") =!= 1)
    assert(round1p1Updates.count() == 4)
    // update keys must be real keys that existed in the external table for this partition
    val bootstrapKeysP1 = bootstrapTableDF.filter(col("partition") === "2026-01-01").select("key")
    assert(round1p1Updates.select("key").except(bootstrapKeysP1).count() == 0)
    assert(round1p1Updates.select("key").distinct().count() == 4)

    val round1p2 = round1.filter(col("partition") === "2026-01-02")
    assert(round1p2.count() == 3)
    assert(round1p2.filter(col("key_round") =!= 1).count() == 0) // all fresh inserts, no updates

    // Round 2: 2 inserts + 3 updates into 2026-01-03.
    val round2 = keyRoundSuffix(spark.read.parquet(s"$incOutputPath/2"))
    assert(round2.count() == 5)
    assert(round2.filter(col("round") =!= 2).count() == 0)
    assert(round2.filter(col("key_round") === 2).count() == 2)
    val round2Updates = round2.filter(col("key_round") =!= 2)
    assert(round2Updates.count() == 3)
    val bootstrapKeysP3 = bootstrapTableDF.filter(col("partition") === "2026-01-03").select("key")
    assert(round2Updates.select("key").except(bootstrapKeysP3).count() == 0)

    // No two rounds' fresh inserts collide.
    val round1InsertKeys = round1.filter(col("key_round") === 1).select("key")
    val round2InsertKeys = round2.filter(col("key_round") === 2).select("key")
    assert(round1InsertKeys.intersect(round2InsertKeys).count() == 0)
  }

  test("externalBootstrap.suffixKeyWithPartitionPath produces <uuid>_<partition> insert keys") {
    val bootstrapSpec = FineGrainedWorkloadSpec.fromJsonString("""
      |{"bootstrap": {"startDate": "2026-02-01", "endDate": "2026-02-01", "totalRecords": 5}}
      |""".stripMargin)
    val rawInputPath = s"file://${workDir.toAbsolutePath}/suffix_raw_input"
    new ChangeDataGenerator(spark, bootstrapSpec.totalRounds)
      .generateFineGrainedWorkload(rawInputPath, bootstrapSpec, recordSize = 256)

    val tablePath = s"file://${workDir.toAbsolutePath}/suffix_hudi_table"
    spark.read
      .parquet(s"$rawInputPath/0")
      .write
      .format("hudi")
      .option("hoodie.table.name", "external_bootstrap_suffix_test")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "key")
      .option("hoodie.datasource.write.partitionpath.field", "partition")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    val incSpec = FineGrainedWorkloadSpec.fromJsonString(s"""
      |{
      |  "externalBootstrap": {"tablePath": "$tablePath", "suffixKeyWithPartitionPath": true},
      |  "commits": [{"2026-02-01": {"inserts": 4}}]
      |}""".stripMargin)
    val incOutputPath = s"file://${workDir.toAbsolutePath}/suffix_inc_output"
    new ChangeDataGenerator(spark, incSpec.totalRounds)
      .generateFineGrainedWorkload(incOutputPath, incSpec, recordSize = 256)

    val round1 = spark.read.parquet(s"$incOutputPath/1")
    assert(round1.count() == 4)
    assert(round1.filter(col("key").endsWith("_2026-02-01")).count() == 4)
  }
}
