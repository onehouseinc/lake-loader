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

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class UnitTestSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("UnitTestSuite")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.default.parallelism", "4")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("genParallelRDD should generate expected number of elements for range 0-100") {
    val end = 100L
    val targetParallelism = 4
    val rdd = ChangeDataGenerator.genParallelRDD(spark, targetParallelism, 0, end)
    val collected = rdd.collect()

    collected.length shouldBe end
    collected.distinct.length shouldBe collected.length
    collected.sorted shouldBe (0L until end).toArray

    // Each partition should have data
    val partitionSizes = rdd.mapPartitions(iter => Iterator(iter.size)).collect()
    partitionSizes.foreach(_ should be > 0)
  }

  test("genParallelRDD should work with small ranges") {
    val end = 10L
    val targetParallelism = 2
    val rdd = ChangeDataGenerator.genParallelRDD(spark, targetParallelism, 0, end)
    val collected = rdd.collect()

    collected.length shouldBe end
    rdd.getNumPartitions shouldBe targetParallelism
    collected.distinct.length shouldBe collected.length
    collected.sorted shouldBe (0L until end).toArray

    // Each partition should have data
    val partitionSizes = rdd.mapPartitions(iter => Iterator(iter.size)).collect()
    partitionSizes.foreach(_ should be > 0)
  }

  test("genParallelRDD should work with large ranges") {
    val end = 10000L
    val targetParallelism = 8
    val rdd = ChangeDataGenerator.genParallelRDD(spark, targetParallelism, 0, end)
    val collected = rdd.collect()

    collected.length shouldBe end
    rdd.getNumPartitions shouldBe targetParallelism
    collected.distinct.length shouldBe collected.length
    collected.sorted shouldBe (0L until end).toArray

    // Each partition should have data
    val partitionSizes = rdd.mapPartitions(iter => Iterator(iter.size)).collect()
    partitionSizes.foreach(_ should be > 0)
  }

  test("genParallelRDD should generate exactly targetParallelism partitions") {
    val targetParallelism = 5
    val end = 100L
    val rdd = ChangeDataGenerator.genParallelRDD(spark, targetParallelism, 0, end)

    // Should have exactly targetParallelism partitions
    rdd.getNumPartitions shouldBe targetParallelism
  }

  test("genParallelRDD should handle non-divisible end values correctly") {
    val end = 97L
    val targetParallelism = 7
    val rdd = ChangeDataGenerator.genParallelRDD(spark, targetParallelism, 0, end)
    val collected = rdd.collect().sorted

    collected.length shouldBe end
    collected shouldBe (0L until end).toArray
    collected.distinct.length shouldBe collected.length
    val partitionSizes = rdd.mapPartitions(iter => Iterator(iter.size)).collect()
    partitionSizes.foreach(_ should be > 0)
  }

  test("genParallelRDD should work with non-zero start and non-divisible range") {
    val start = 50L
    val end = 147L // 97 elements total
    val targetParallelism = 7
    val rdd = ChangeDataGenerator.genParallelRDD(spark, targetParallelism, start, end)
    val collected = rdd.collect().sorted

    // Verify correctness of collected
    val expectedCount = end - start
    collected.length shouldBe expectedCount
    collected shouldBe (start until end).toArray
    collected.distinct.length shouldBe collected.length

    // Check partition size distribution
    // First partition should have 13 elements and last 6 partitions should have 14 elements
    rdd.getNumPartitions shouldBe targetParallelism
    val partitionSizes = rdd.mapPartitions(iter => Iterator(iter.size)).collect()
    partitionSizes.length shouldBe targetParallelism
    val expectedSizes = Array(13, 14, 14, 14, 14, 14, 14)
    partitionSizes shouldBe expectedSizes
  }
}
