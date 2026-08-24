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

import ai.onehouse.lakeloader.utils.ScaleTransform
import org.scalatest.funsuite.AnyFunSuite

class ScaleTransformSpec extends AnyFunSuite {

  test("scaleRecordsPerRound multiplies each entry by factor") {
    val out = ScaleTransform.scaleRecordsPerRound(List(1000L, 2000L, 500L), 0.1)
    assert(out == List(100L, 200L, 50L))
  }

  test("scaleRecordsPerRound clamps to at least 1") {
    val out = ScaleTransform.scaleRecordsPerRound(List(1000L, 5L, 50L), 0.01)
    // 1000*0.01=10; 5*0.01=0.05→0→clamp 1; 50*0.01=0.5→0→clamp 1
    assert(out == List(10L, 1L, 1L))
  }

  test("scaleRecordsPerRound preserves length") {
    val src = (1L to 42L).toList
    val out = ScaleTransform.scaleRecordsPerRound(src, 0.001)
    assert(out.size == src.size)
  }

  test("scaleRecordsPerRound scales up") {
    val out = ScaleTransform.scaleRecordsPerRound(List(100L, 200L), 5.0)
    assert(out == List(500L, 1000L))
  }

  test("scaleRecordsPerRound rejects non-positive factor") {
    intercept[IllegalArgumentException] {
      ScaleTransform.scaleRecordsPerRound(List(100L), 0.0)
    }
    intercept[IllegalArgumentException] {
      ScaleTransform.scaleRecordsPerRound(List(100L), -0.1)
    }
  }

  test("scalePartitionDistribution truncates and re-normalizes when scaling down") {
    val src = List(0.5, 0.3, 0.15, 0.05)
    val out = ScaleTransform.scalePartitionDistribution(src, targetPartitions = 2, fittedShape = 2.0)
    // Take (0.5, 0.3), sum=0.8, normalize → (0.625, 0.375)
    assert(math.abs(out(0) - 0.625) < 1e-9)
    assert(math.abs(out(1) - 0.375) < 1e-9)
    assert(math.abs(out.sum - 1.0) < 1e-9)
  }

  test("scalePartitionDistribution extrapolates using fitted zipf shape when scaling up") {
    val src = List(0.6, 0.3, 0.1) // 3 partitions
    val out = ScaleTransform.scalePartitionDistribution(src, targetPartitions = 10, fittedShape = 2.0)
    assert(out.size == 10)
    assert(math.abs(out.sum - 1.0) < 1e-9)
    // Head should still dominate for shape=2
    assert(out.head > 0.5)
    // Strictly monotonic descending
    assert(out.zip(out.tail).forall { case (a, b) => a >= b })
  }

  test("scalePartitionDistribution falls back to uniform when shape is 0") {
    val src = List(0.5, 0.5)
    val out = ScaleTransform.scalePartitionDistribution(src, targetPartitions = 5, fittedShape = 0.0)
    assert(out == List.fill(5)(0.2))
  }

  test("scalePartitionDistribution preserves source when target equals source size") {
    val src = List(0.4, 0.3, 0.2, 0.1)
    val out = ScaleTransform.scalePartitionDistribution(src, targetPartitions = 4, fittedShape = 1.5)
    // Truncation branch: takes all 4, sum=1.0, normalizes → unchanged
    src.zip(out).foreach { case (a, b) => assert(math.abs(a - b) < 1e-9) }
  }

  test("scalePartitionDistribution returns empty for empty input") {
    val out = ScaleTransform.scalePartitionDistribution(Nil, 10, 2.0)
    assert(out.isEmpty)
  }

  test("scalePartitionDistribution handles all-zero source by returning uniform") {
    val src = List(0.0, 0.0, 0.0)
    val out = ScaleTransform.scalePartitionDistribution(src, targetPartitions = 2, fittedShape = 2.0)
    assert(out == List(0.5, 0.5))
  }

  test("scaleNumPartitionsToUpdate preserves fraction, ceilings up") {
    // Source: 21/3000 = 0.7% → target=300 → 0.7% * 300 = 2.1 → ceil = 3
    assert(ScaleTransform.scaleNumPartitionsToUpdate(21, 3000, 300) == 3)
    // Source: 7/365, target=30 → 7/365*30 ≈ 0.575 → ceil = 1
    assert(ScaleTransform.scaleNumPartitionsToUpdate(7, 365, 30) == 1)
    // Scaling up: 3/10 → 3/10*100 = 30
    assert(ScaleTransform.scaleNumPartitionsToUpdate(3, 10, 100) == 30)
  }

  test("scaleNumPartitionsToUpdate returns 0 when source had no updates") {
    assert(ScaleTransform.scaleNumPartitionsToUpdate(0, 3000, 300) == 0)
  }

  test("scaleNumPartitionsToUpdate caps at targetTotal") {
    // Source: 5/5 (all partitions), target=3 → math says 3, cap 3
    assert(ScaleTransform.scaleNumPartitionsToUpdate(5, 5, 3) == 3)
  }
}
