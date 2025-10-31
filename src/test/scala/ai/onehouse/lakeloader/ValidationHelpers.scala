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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Validation helpers for testing change data generation and key/column preservation across rounds.
 * Provides modular, reusable validation functions to verify data generation behavior.
 */
object ValidationHelpers {

  /**
   * Loads and caches data from two rounds for comparison.
   *
   * @param spark Spark session
   * @param path  Base path containing round data
   * @param firstRound First round number to load
   * @param secondRound Second round number to load
   * @return Tuple of (firstDataFrame, secondDataFrame)
   */
  def loadRounds(
      spark: SparkSession,
      path: String,
      firstRound: Int,
      secondRound: Int): (DataFrame, DataFrame) = {
    val df1 = spark.read.parquet(s"$path/$firstRound").cache()
    val df2 = spark.read.parquet(s"$path/$secondRound").cache()

    (df1, df2)
  }

  /**
   * Loads and caches data from three rounds for comparison.
   *
   * @param spark Spark session
   * @param path  Base path containing round data
   * @param firstRound First round number to load
   * @param secondRound Second round number to load
   * @param thirdRound Third round number to load
   * @return Tuple of (firstDataFrame, secondDataFrame, thirdDataFrame)
   */
  def loadThreeRounds(
      spark: SparkSession,
      path: String,
      firstRound: Int,
      secondRound: Int,
      thirdRound: Int): (DataFrame, DataFrame, DataFrame) = {
    val df1 = spark.read.parquet(s"$path/$firstRound").cache()
    val df2 = spark.read.parquet(s"$path/$secondRound").cache()
    val df3 = spark.read.parquet(s"$path/$thirdRound").cache()

    (df1, df2, df3)
  }

  /**
   * Joins two round dataframes on specified keys.
   *
   * @param df1 First dataframe to join
   * @param df2 Second dataframe to join
   * @param joinKeys Keys to join on (default: "key")
   * @param joinType Type of join (default: "inner")
   * @return Joined and cached dataframe with aliases "r1" and "r2"
   */
  def joinRounds(
      df1: DataFrame,
      df2: DataFrame,
      joinKeys: Seq[String] = Seq("key"),
      joinType: String = "inner"): DataFrame = {
    df1
      .alias("r1")
      .join(df2.alias("r2"), joinKeys, joinType)
      .cache()
  }

  /**
   * Validates that specified columns are preserved (unchanged) across rounds.
   *
   * @param joinedDf Joined dataframe with r1 and r2 aliases
   * @param columns Columns that should be preserved
   * @throws AssertionError if any columns have changed
   */
  def validateColumnsPreserved(joinedDf: DataFrame, columns: Set[String]): Unit = {
    val mismatches = columns.flatMap { col =>
      val mismatchCount = joinedDf.filter(s"r1.$col != r2.$col").count()
      if (mismatchCount > 0) {
        Some((col, mismatchCount))
      } else {
        None
      }
    }

    if (mismatches.nonEmpty) {
      val details = mismatches.map { case (col, count) => s"$col ($count records)" }.mkString(", ")
      throw new AssertionError(s"Columns changed but should be preserved: $details")
    }
  }

  /**
   * Validates that at least some non-preserved columns have changed across rounds.
   *
   * @param joinedDf Joined dataframe with r1 and r2 aliases
   * @param allColumns All column names in the schema
   * @param excludeColumns Columns to exclude from this check (preserved columns, round, ts)
   * @throws AssertionError if no changes were detected in non-preserved columns
   */
  def validateColumnsChanged(
      joinedDf: DataFrame,
      allColumns: Set[String],
      excludeColumns: Set[String]): Unit = {
    val columnsToCheck = allColumns -- excludeColumns

    if (columnsToCheck.isEmpty) {
      return
    }

    val totalChanges = columnsToCheck.map { col =>
      joinedDf
        .filter(
          s"r1.$col != r2.$col OR (r1.$col IS NULL AND r2.$col IS NOT NULL) OR (r1.$col IS NOT NULL AND r2.$col IS NULL)")
        .count()
    }.sum

    if (totalChanges == 0) {
      throw new AssertionError(
        s"No changes detected in non-preserved columns: ${columnsToCheck.mkString(", ")}")
    }
  }

  /**
   * Validates that the update ratio (overlapping keys) is within expected range.
   *
   * @param updatedCount Number of records that were updated (overlapping keys)
   * @param totalCount Total number of records in first round
   * @param expectedMin Minimum expected ratio
   * @param expectedMax Maximum expected ratio
   * @throws AssertionError if the update ratio is outside expected range
   */
  def validateUpdateRatio(
      updatedCount: Long,
      totalCount: Long,
      expectedMin: Double,
      expectedMax: Double): Unit = {
    val actualRatio = updatedCount.toDouble / totalCount.toDouble

    if (actualRatio < expectedMin || actualRatio > expectedMax) {
      throw new AssertionError(
        s"Update ratio $actualRatio is outside expected range [$expectedMin, $expectedMax] " +
          s"($updatedCount updated out of $totalCount)")
    }
  }

  /**
   * Validates that the key overlap ratio between rounds is within expected range.
   *
   * @param overlapCount Number of overlapping keys
   * @param round1Count Total number of records in second round
   * @param expectedMin Minimum expected overlap ratio
   * @param expectedMax Maximum expected overlap ratio
   * @throws AssertionError if the overlap ratio is outside expected range
   */
  def validateKeyOverlapRatio(
      overlapCount: Long,
      round1Count: Long,
      expectedMin: Double,
      expectedMax: Double): Unit = {
    val actualRatio = overlapCount.toDouble / round1Count.toDouble

    if (actualRatio < expectedMin || actualRatio > expectedMax) {
      throw new AssertionError(
        s"Key overlap ratio $actualRatio is outside expected range [$expectedMin, $expectedMax] " +
          s"($overlapCount overlapping keys out of $round1Count in round 1)")
    }
  }

  /**
   * Validates that additional merge condition columns are preserved for all overlapping keys.
   * This validation to ensure merge condition columns maintain their values.
   *
   * @param joinedDf Joined dataframe with r1 and r2 aliases
   * @param additionalColumns Additional merge condition columns to validate
   * @throws AssertionError if any additional merge column has changed (fails fast on first failure)
   */
  def validateAdditionalMergeConditionColumnsPreserved(
      joinedDf: DataFrame,
      additionalColumns: Seq[String]): Unit = {
    if (additionalColumns.isEmpty) {
      return
    }

    val overlapCount = joinedDf.count()
    if (overlapCount == 0) {
      return
    }

    additionalColumns.foreach { col =>
      val mismatchCount = joinedDf
        .filter(
          s"r1.$col != r2.$col OR (r1.$col IS NULL AND r2.$col IS NOT NULL) OR (r1.$col IS NOT NULL AND r2.$col IS NULL)")
        .count()

      if (mismatchCount > 0) {
        throw new AssertionError(
          s"Additional merge column '$col' changed in $mismatchCount out of $overlapCount records, " +
            s"but should be preserved for all overlapping keys")
      }
    }
  }

  /**
   * Validates update behavior of a target round against the union of two previous rounds.
   * This ensures that updates in the target round are drawn from all keys seen in previous rounds.
   *
   * @param round0 First round dataframe
   * @param round1 Second round dataframe
   * @param round2 Target round to validate
   * @param expectedUpdateRatioMin Minimum expected ratio of (round0 ∪ round1) keys that appear in round2
   * @param expectedUpdateRatioMax Maximum expected ratio of (round0 ∪ round1) keys that appear in round2
   * @param preservedColumns Columns that should remain unchanged for overlapping keys
   * @param additionalMergeColumns Additional merge condition columns to validate
   */
  def validateUpdateBehaviorAgainstUnion(
      round0: DataFrame,
      round1: DataFrame,
      round2: DataFrame,
      expectedUpdateRatioMin: Double,
      expectedUpdateRatioMax: Double,
      preservedColumns: Set[String],
      additionalMergeColumns: Seq[String]): Unit = {

    // Get union of keys from round0 and round1
    val round0Keys = round0.select("key").distinct()
    val round1Keys = round1.select("key").distinct()
    val unionKeys = round0Keys.union(round1Keys).distinct().cache()
    val unionCount = unionKeys.count()

    // Find how many of the union keys appear in round2
    val round2Keys = round2.select("key").distinct()
    val overlapKeys = unionKeys.join(round2Keys, Seq("key"), "inner").cache()
    val overlapCount = overlapKeys.count()

    // Validate update ratio (against target round size, not union size)
    validateUpdateRatio(
      overlapCount,
      round2.count(),
      expectedUpdateRatioMin,
      expectedUpdateRatioMax)

    if (overlapCount > 0) {
      // For overlapping keys, join with round0 union round1 to validate preserved columns
      // We need to get the actual records from union, preferring round1 over round0 for duplicates
      val unionRecords = round1
        .union(round0.join(round1.select("key"), Seq("key"), "left_anti"))
        .cache()

      val joinedDf = joinRounds(unionRecords, round2, joinKeys = Seq("key"))

      // Validate preserved columns remain unchanged
      validateColumnsPreserved(joinedDf, preservedColumns)

      // Validate non-preserved columns actually changed
      val allColumns = round2.columns.toSet[String]
      val excludeColumns = preservedColumns ++ Set("round", "ts")
      validateColumnsChanged(joinedDf, allColumns, excludeColumns)

      // Validate additional merge condition columns are preserved
      validateAdditionalMergeConditionColumnsPreserved(joinedDf, additionalMergeColumns)

      // Cleanup
      joinedDf.unpersist()
      unionRecords.unpersist()
    }

    // Cleanup
    unionKeys.unpersist()
    overlapKeys.unpersist()
  }
}
