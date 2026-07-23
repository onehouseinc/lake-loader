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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import scala.reflect.io.Directory

/**
 * End-to-end test for the fine-grained workload spec path: generates a tiny multi-round
 * workload on local Spark and asserts the per-partition insert/update counts of every round
 * match the spec exactly.
 *
 * Inserts and updates are distinguished by the round suffix embedded in generated keys
 * ("uuid-%03d" of the round the key was created in): a row in round k whose key suffix is k is
 * an insert; any other suffix means the row updates a key created in an earlier round.
 */
class FineGrainedWorkloadGenTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var workDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    workDir = Files.createTempDirectory("fine_grained_workload_test")
    spark = SparkSession.builder
      .master("local[2]")
      .appName("FineGrainedWorkloadGenTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
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

  private def keyRoundSuffix(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    df.withColumn("key_round", expr("cast(substring(key, -3, 3) as int)"))
  }

  test("generated rounds match the spec's exact per-partition insert/update counts") {
    import org.apache.spark.sql.functions._

    val spec = FineGrainedWorkloadSpec.fromJsonString("""
      |{
      |  "bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-03", "totalRecords": 100},
      |  "commits": [
      |    {"2026-01-01": {"inserts": 15, "updates": 6}, "2026-01-04": {"inserts": 20}},
      |    {"2026-01-04": {"updates": 9}, "2026-01-02": {"inserts": 4, "updates": 3}}
      |  ]
      |}""".stripMargin)

    val outputPath = s"file://${workDir.toAbsolutePath}/workload"
    new ChangeDataGenerator(spark, spec.totalRounds)
      .generateFineGrainedWorkload(outputPath, spec, recordSize = 256)

    // Round 0: 100 records split evenly across the 3 bootstrap partitions (34, 33, 33).
    val round0 = keyRoundSuffix(spark.read.parquet(s"$outputPath/0"))
    assert(round0.count() == 100)
    assert(round0.filter(col("round") =!= 0 || col("key_round") =!= 0).count() == 0)
    val round0Counts = round0
      .groupBy("partition")
      .count()
      .collect()
      .map(r => r.getAs[String]("partition") -> r.getAs[Long]("count"))
      .toMap
    assert(
      round0Counts ==
        Map("2026-01-01" -> 34L, "2026-01-02" -> 33L, "2026-01-03" -> 33L))

    // Round 1: 15 inserts + 6 updates into 2026-01-01, 20 inserts opening 2026-01-04.
    val round1 = keyRoundSuffix(spark.read.parquet(s"$outputPath/1"))
    assert(round1.count() == 41)
    assert(round1.filter(col("round") =!= 1).count() == 0)
    val r1p1 = round1.filter(col("partition") === "2026-01-01")
    assert(r1p1.filter(col("key_round") === 1).count() == 15)
    val r1p1Updates = r1p1.filter(col("key_round") =!= 1)
    assert(r1p1Updates.count() == 6)
    assert(r1p1Updates.select("key").distinct().count() == 6)
    // updated keys must exist in round 0 within the same partition
    val round0Keys = round0
      .filter(col("partition") === "2026-01-01")
      .select("key")
    assert(r1p1Updates.select("key").except(round0Keys).count() == 0)
    val r1p4 = round1.filter(col("partition") === "2026-01-04")
    assert(r1p4.count() == 20)
    assert(r1p4.filter(col("key_round") =!= 1).count() == 0)

    // Round 2: 9 updates to keys inserted into 2026-01-04 by round 1; 4 inserts + 3 updates
    // in 2026-01-02.
    val round2 = keyRoundSuffix(spark.read.parquet(s"$outputPath/2"))
    assert(round2.count() == 16)
    assert(round2.filter(col("round") =!= 2).count() == 0)
    val r2p4 = round2.filter(col("partition") === "2026-01-04")
    assert(r2p4.count() == 9)
    assert(r2p4.filter(col("key_round") =!= 1).count() == 0)
    assert(r2p4.select("key").except(r1p4.select("key")).count() == 0)
    val r2p2 = round2.filter(col("partition") === "2026-01-02")
    assert(r2p2.filter(col("key_round") === 2).count() == 4)
    val r2p2Updates = r2p2.filter(col("key_round") =!= 2)
    assert(r2p2Updates.count() == 3)
    assert(r2p2Updates.filter(col("key_round") =!= 0).count() == 0)

    // No partitions beyond the ones the spec names.
    val allPartitions = round0
      .select("partition")
      .union(round1.select("partition"))
      .union(round2.select("partition"))
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSet
    assert(
      allPartitions ==
        Set("2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"))
  }

  test("updates requesting more keys than exist update all available keys with a warning") {
    val spec = FineGrainedWorkloadSpec.fromJsonString("""
      |{
      |  "bootstrap": {"startDate": "2026-03-01", "endDate": "2026-03-02", "totalRecords": 10},
      |  "commits": [
      |    {"2026-03-01": {"updates": 50}}
      |  ]
      |}""".stripMargin)

    val outputPath = s"file://${workDir.toAbsolutePath}/workload_overshoot"
    new ChangeDataGenerator(spark, spec.totalRounds)
      .generateFineGrainedWorkload(outputPath, spec, recordSize = 256)

    import org.apache.spark.sql.functions._
    val round1 = spark.read.parquet(s"$outputPath/1")
    // only 5 keys exist in 2026-03-01 (10 records over 2 partitions) — all get updated
    assert(round1.count() == 5)
    assert(round1.filter(col("partition") =!= "2026-03-01").count() == 0)
    assert(round1.select("key").distinct().count() == 5)
  }
}
