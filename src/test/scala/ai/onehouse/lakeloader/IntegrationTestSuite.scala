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

import ai.onehouse.lakeloader.configs.{KeyTypes, UpdatePatterns}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class IntegrationTestSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("ChangeDataGeneratorTestSuite")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "5")
      .config("spark.default.parallelism", "5")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Random (Uniform) updates without additional merge condition columns") {
    val tempDir = Files.createTempDirectory("test-random-no-merge").toString
    val generator = new ChangeDataGenerator(spark, numRounds = 2)

    generator.generateWorkload(
      path = tempDir,
      roundsDistribution = List(1000L, 1000L),
      numColumns = 10,
      recordSize = 512,
      updateRatio = 0.5,
      totalPartitions = 2,
      targetDataFileSize = 10 * 1024 * 1024,
      keyType = KeyTypes.Random,
      updatePatterns = UpdatePatterns.Uniform,
      numPartitionsToUpdate = 2,
      additionalMergeConditionColumns = Seq.empty)

    validateUpdateBehavior(
      tempDir,
      firstRound = 0,
      secondRound = 1,
      expectedUpdateRatioMin = 0.4,
      expectedUpdateRatioMax = 0.6,
      preservedColumns = Set("key", "partition"),
      additionalMergeColumns = Seq.empty)
  }

  test("Random (Uniform) updates with additional merge condition columns") {
    val tempDir = Files.createTempDirectory("test-random-with-merge").toString
    val generator = new ChangeDataGenerator(spark, numRounds = 3)

    generator.generateWorkload(
      path = tempDir,
      roundsDistribution = List(1000L, 1000L, 1000L),
      numColumns = 20,
      recordSize = 512,
      updateRatio = 0.5,
      totalPartitions = 2,
      targetDataFileSize = 10 * 1024 * 1024,
      keyType = KeyTypes.Random,
      updatePatterns = UpdatePatterns.Uniform,
      numPartitionsToUpdate = 2,
      additionalMergeConditionColumns = Seq("longField27"))

    // Validate round 0 -> 1
    validateUpdateBehavior(
      tempDir,
      firstRound = 0,
      secondRound = 1,
      expectedUpdateRatioMin = 0.4,
      expectedUpdateRatioMax = 0.6,
      preservedColumns = Set("key", "partition", "longField27"),
      additionalMergeColumns = Seq("longField27"))

    // Validate round 2 against union of round 0 and round 1
    val (round0, round1, round2) = ValidationHelpers.loadThreeRounds(spark, tempDir, 0, 1, 2)
    ValidationHelpers.validateUpdateBehaviorAgainstUnion(
      round0,
      round1,
      round2,
      expectedUpdateRatioMin = 0.4,
      expectedUpdateRatioMax = 0.6,
      preservedColumns = Set("key", "partition", "longField27"),
      additionalMergeColumns = Seq("longField27"))

    // Cleanup
    round0.unpersist()
    round1.unpersist()
    round2.unpersist()
  }

  test("Zipf updates without additional merge condition columns") {
    val tempDir = Files.createTempDirectory("test-zipf-no-merge").toString
    val generator = new ChangeDataGenerator(spark, numRounds = 2)

    generator.generateWorkload(
      path = tempDir,
      roundsDistribution = List(1000L, 1000L),
      numColumns = 10,
      recordSize = 512,
      updateRatio = 0.5,
      totalPartitions = 2,
      targetDataFileSize = 10 * 1024 * 1024,
      keyType = KeyTypes.Random,
      updatePatterns = UpdatePatterns.Zipf,
      numPartitionsToUpdate = 2,
      additionalMergeConditionColumns = Seq.empty)

    validateUpdateBehavior(
      tempDir,
      firstRound = 0,
      secondRound = 1,
      expectedUpdateRatioMin = 0.4,
      expectedUpdateRatioMax = 0.6,
      preservedColumns = Set("key", "partition"),
      additionalMergeColumns = Seq.empty)
  }

  test("Zipf updates with additional merge condition columns") {
    val tempDir = Files.createTempDirectory("test-zipf-with-merge").toString
    val generator = new ChangeDataGenerator(spark, numRounds = 2)

    generator.generateWorkload(
      path = tempDir,
      roundsDistribution = List(1000L, 1000L),
      numColumns = 10,
      recordSize = 512,
      updateRatio = 0.5,
      totalPartitions = 2,
      targetDataFileSize = 10 * 1024 * 1024,
      keyType = KeyTypes.Random,
      updatePatterns = UpdatePatterns.Zipf,
      numPartitionsToUpdate = 2,
      additionalMergeConditionColumns = Seq("textField10"))

    validateUpdateBehavior(
      tempDir,
      firstRound = 0,
      secondRound = 1,
      expectedUpdateRatioMin = 0.4,
      expectedUpdateRatioMax = 0.6,
      preservedColumns = Set("key", "partition", "textField10"),
      additionalMergeColumns = Seq("textField10"))
  }

  test("updateRatio 1.0 - all updates, no new inserts") {
    val tempDir = Files.createTempDirectory("test-ratio-1.0").toString
    val generator = new ChangeDataGenerator(spark, numRounds = 2)

    generator.generateWorkload(
      path = tempDir,
      roundsDistribution = List(1000L, 1000L),
      numColumns = 10,
      recordSize = 512,
      updateRatio = 1.0,
      totalPartitions = 2,
      targetDataFileSize = 10 * 1024 * 1024,
      keyType = KeyTypes.Random,
      updatePatterns = UpdatePatterns.Uniform,
      numPartitionsToUpdate = 2,
      additionalMergeConditionColumns = Seq("textField10"))

    validateKeyOverlap(
      tempDir,
      expectedOverlapRatioMin = 0.95,
      expectedOverlapRatioMax = 1.0,
      additionalMergeColumns = Seq("textField10"))
  }

  test("updateRatio 0.0 - all inserts, no updates") {
    val tempDir = Files.createTempDirectory("test-ratio-0.0").toString
    val generator = new ChangeDataGenerator(spark, numRounds = 2)

    generator.generateWorkload(
      path = tempDir,
      roundsDistribution = List(1000L, 1000L),
      numColumns = 10,
      recordSize = 512,
      updateRatio = 0.0,
      totalPartitions = 2,
      targetDataFileSize = 10 * 1024 * 1024,
      keyType = KeyTypes.Random,
      updatePatterns = UpdatePatterns.Uniform,
      numPartitionsToUpdate = 2,
      additionalMergeConditionColumns = Seq("textField10"))

    validateKeyOverlap(
      tempDir,
      expectedOverlapRatioMin = 0.0,
      expectedOverlapRatioMax = 0.05,
      additionalMergeColumns = Seq("textField10"))
  }

  private def validateUpdateBehavior(
      path: String,
      firstRound: Int,
      secondRound: Int,
      expectedUpdateRatioMin: Double,
      expectedUpdateRatioMax: Double,
      preservedColumns: Set[String],
      additionalMergeColumns: Seq[String]): Unit = {

    // Load data from both rounds
    val (firstDf, secondDf) = ValidationHelpers.loadRounds(spark, path, firstRound, secondRound)

    // Join rounds to find updated records
    val joinedDf = ValidationHelpers.joinRounds(firstDf, secondDf, joinKeys = Seq("key"))
    val updatedCount = joinedDf.count()

    // Validate update ratio
    ValidationHelpers.validateUpdateRatio(
      updatedCount,
      firstDf.count(),
      expectedUpdateRatioMin,
      expectedUpdateRatioMax)

    if (updatedCount > 0) {
      // Validate preserved columns remain unchanged
      ValidationHelpers.validateColumnsPreserved(joinedDf, preservedColumns)

      // Validate non-preserved columns actually changed
      val allColumns = firstDf.columns.toSet[String]
      val excludeColumns = preservedColumns ++ Set("round", "ts")
      ValidationHelpers.validateColumnsChanged(joinedDf, allColumns, excludeColumns)

      // Validate additional merge condition columns are preserved
      ValidationHelpers.validateAdditionalMergeConditionColumnsPreserved(
        joinedDf,
        additionalMergeColumns)
    }

    // Cleanup
    firstDf.unpersist()
    secondDf.unpersist()
    joinedDf.unpersist()
  }

  private def validateKeyOverlap(
      path: String,
      expectedOverlapRatioMin: Double,
      expectedOverlapRatioMax: Double,
      additionalMergeColumns: Seq[String]): Unit = {

    // Load data from both rounds
    val (firstDf, secondDf) = ValidationHelpers.loadRounds(spark, path, 0, 1)

    // Join rounds to find overlapping keys
    val joinedDf = ValidationHelpers.joinRounds(firstDf, secondDf, joinKeys = Seq("key"))
    val overlapCount = joinedDf.count()

    // Validate key overlap ratio
    ValidationHelpers.validateKeyOverlapRatio(
      overlapCount,
      secondDf.count(),
      expectedOverlapRatioMin,
      expectedOverlapRatioMax)

    // Validate additional merge columns match for overlapping keys
    if (overlapCount > 0 && additionalMergeColumns.nonEmpty) {
      ValidationHelpers.validateAdditionalMergeConditionColumnsPreserved(
        joinedDf,
        additionalMergeColumns)
    }

    // Cleanup
    firstDf.unpersist()
    secondDf.unpersist()
    joinedDf.unpersist()
  }
}
