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

import ai.onehouse.lakeloader.WorkloadSynthesizer.{DerivedConfig, InferredColumnCount, SuppliedSchema}
import ai.onehouse.lakeloader.configs.{DatagenConfig, KeyTypes, ResizerConfig, UpdatePatterns}
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser
import org.scalatest.funsuite.AnyFunSuite

class WorkloadResizerSpec extends AnyFunSuite {

  private def sample(
      numRounds: Int = 5,
      recordsPerRound: List[Long] = List(1000L, 2000L, 1500L, 2500L, 1000L),
      totalPartitions: Int = 100,
      updateRatio: Double = 0.3,
      numPartitionsToUpdate: Int = 20,
      partitionDistribution: List[Double] = List(0.5, 0.25, 0.15, 0.05, 0.05),
      zipfShape: Double = 2.0,
      updatePattern: UpdatePatterns.UpdatePatterns = UpdatePatterns.Zipf): DerivedConfig =
    DerivedConfig(
      numRounds = numRounds,
      recordsPerRound = recordsPerRound,
      medianRecordsPerRound = 1500L,
      totalPartitions = totalPartitions,
      updateRatio = updateRatio,
      numPartitionsToUpdate = numPartitionsToUpdate,
      recordSize = 640,
      targetDataFileSize = 128 * 1024 * 1024,
      updatePattern = updatePattern,
      zipfShape = zipfShape,
      partitionDistribution = partitionDistribution,
      round0PartitionDistribution = None,
      keyType = KeyTypes.Random,
      keyTypeSource = "test",
      recordKeyField = Some("id"),
      schemaChoice = InferredColumnCount(10),
      auditNotes = Seq("test note"))

  test("applyScale with factor=1.0 and no partition change is a no-op") {
    val src = sample()
    val out = WorkloadResizer.applyScale(src, ResizerConfig(scaleFactor = 1.0))
    assert(out.recordsPerRound == src.recordsPerRound)
    assert(out.totalPartitions == src.totalPartitions)
    assert(out.numPartitionsToUpdate == src.numPartitionsToUpdate)
    assert(out.partitionDistribution == src.partitionDistribution)
    // Invariants
    assert(out.updateRatio == src.updateRatio)
    assert(out.zipfShape == src.zipfShape)
    assert(out.recordSize == src.recordSize)
    assert(out.keyType == src.keyType)
  }

  test("applyScale scales records per round by factor, preserves round count") {
    val src = sample()
    val out = WorkloadResizer.applyScale(src, ResizerConfig(scaleFactor = 0.1))
    assert(out.numRounds == src.numRounds)
    assert(out.recordsPerRound == List(100L, 200L, 150L, 250L, 100L))
    // Median recomputed on the new list: sorted = 100,100,150,200,250 → 150
    assert(out.medianRecordsPerRound == 150L)
  }

  test("applyScale preserves invariants regardless of factor") {
    val src = sample()
    val out = WorkloadResizer.applyScale(src, ResizerConfig(scaleFactor = 0.001))
    assert(out.updateRatio == src.updateRatio)
    assert(out.updatePattern == src.updatePattern)
    assert(out.zipfShape == src.zipfShape)
    assert(out.recordSize == src.recordSize)
    assert(out.targetDataFileSize == src.targetDataFileSize)
    assert(out.keyType == src.keyType)
    assert(out.schemaChoice == src.schemaChoice)
  }

  test("applyScale rescales partition count and dependents") {
    val src = sample(totalPartitions = 3000, numPartitionsToUpdate = 21)
    val out = WorkloadResizer.applyScale(src, ResizerConfig(targetPartitions = Some(300)))
    assert(out.totalPartitions == 300)
    // 21/3000 = 0.007 → 0.007 * 300 = 2.1 → ceil = 3
    assert(out.numPartitionsToUpdate == 3)
    // partition-distribution should be re-derived; sum ≈ 1.0
    // Tolerance loosened for the rounding-to-1e-6 that applyScale applies for output stability
    assert(math.abs(out.partitionDistribution.sum - 1.0) < 1e-3)
  }

  test("applyScale rescales partitions up, extrapolates from fitted zipf shape") {
    val src = sample(
      totalPartitions = 5,
      partitionDistribution = List(0.5, 0.25, 0.15, 0.05, 0.05),
      zipfShape = 2.0,
      numPartitionsToUpdate = 2)
    val out = WorkloadResizer.applyScale(src, ResizerConfig(targetPartitions = Some(100)))
    assert(out.totalPartitions == 100)
    assert(out.partitionDistribution.size == 100)
    // Tolerance loosened for the rounding-to-1e-6 that applyScale applies for output stability
    assert(math.abs(out.partitionDistribution.sum - 1.0) < 1e-3)
    // 2/5 = 0.4, * 100 = 40
    assert(out.numPartitionsToUpdate == 40)
    // Zipf shape still dominates the head
    assert(out.partitionDistribution.head > 0.5)
  }

  test("applyScale combines both scaling axes independently") {
    val src = sample(
      totalPartitions = 1000,
      numPartitionsToUpdate = 50,
      recordsPerRound = List(10000L, 20000L))
    val out = WorkloadResizer.applyScale(src,
      ResizerConfig(scaleFactor = 0.01, targetPartitions = Some(100)))
    assert(out.totalPartitions == 100)
    // 50/1000 = 0.05 * 100 = 5
    assert(out.numPartitionsToUpdate == 5)
    assert(out.recordsPerRound == List(100L, 200L))
  }

  test("applyScale rejects zero or negative scale factor") {
    intercept[IllegalArgumentException] {
      WorkloadResizer.applyScale(sample(), ResizerConfig(scaleFactor = 0.0))
    }
    intercept[IllegalArgumentException] {
      WorkloadResizer.applyScale(sample(), ResizerConfig(scaleFactor = -1.0))
    }
  }

  test("scaled DerivedConfig round-trips through ChangeDataGeneratorParser") {
    val src = sample(totalPartitions = 500, numPartitionsToUpdate = 25)
    val scaled = WorkloadResizer.applyScale(src,
      ResizerConfig(scaleFactor = 0.05, targetPartitions = Some(50)))
    val raw = WorkloadSynthesizer.renderFullFlags(scaled)
    val runnable = raw
      .replace("<fill-in>.avsc", "/tmp/dummy.avsc")
      .replace("<fill-in>", "/tmp/dummy-out")
    val args = runnable.trim.split("\\s+").map(_.replaceAll("^'|'$", ""))
    val parsed = ChangeDataGeneratorParser.parser.parse(args, DatagenConfig())
    assert(parsed.isDefined, s"parser rejected scaled flags:\n$raw")
    val cfg = parsed.get
    assert(cfg.totalPartitions == 50)
    // numRounds preserved from source (5)
    assert(cfg.numberOfRounds == 5)
    assert(cfg.recordSize == 640)
  }

  test("parseSynthDerivedJson round-trips through renderDerivedJson") {
    val src = sample()
    val json = WorkloadSynthesizer.renderDerivedJson(src, "/dummy/path")
    val parsed = WorkloadResizer.parseSynthDerivedJson(json)
    assert(parsed.numRounds == src.numRounds)
    assert(parsed.recordsPerRound == src.recordsPerRound)
    assert(parsed.totalPartitions == src.totalPartitions)
    assert(math.abs(parsed.updateRatio - src.updateRatio) < 1e-9)
    assert(parsed.numPartitionsToUpdate == src.numPartitionsToUpdate)
    assert(parsed.recordSize == src.recordSize)
    assert(parsed.updatePattern == src.updatePattern)
    assert(math.abs(parsed.zipfShape - src.zipfShape) < 1e-9)
    assert(parsed.keyType == src.keyType)
    parsed.partitionDistribution.zip(src.partitionDistribution).foreach { case (a, b) =>
      assert(math.abs(a - b) < 1e-9)
    }
    assert(parsed.schemaChoice == src.schemaChoice)
  }

  test("parseSynthDerivedJson handles SuppliedSchema") {
    val src = sample().copy(schemaChoice = SuppliedSchema("/customer/schema.avsc"))
    val json = WorkloadSynthesizer.renderDerivedJson(src, "/dummy/path")
    val parsed = WorkloadResizer.parseSynthDerivedJson(json)
    assert(parsed.schemaChoice == SuppliedSchema("/customer/schema.avsc"))
  }

  test("parseSynthDerivedJson handles round0PartitionDistribution=null") {
    val src = sample() // round0 is None
    val json = WorkloadSynthesizer.renderDerivedJson(src, "/dummy/path")
    assert(json.contains("\"round0PartitionDistribution\": null"))
    val parsed = WorkloadResizer.parseSynthDerivedJson(json)
    assert(parsed.round0PartitionDistribution.isEmpty)
  }

  test("parseSynthDerivedJson handles non-null round0PartitionDistribution") {
    val src = sample().copy(round0PartitionDistribution = Some(List(0.5, 0.3, 0.2)))
    val json = WorkloadSynthesizer.renderDerivedJson(src, "/dummy/path")
    val parsed = WorkloadResizer.parseSynthDerivedJson(json)
    assert(parsed.round0PartitionDistribution.isDefined)
    parsed.round0PartitionDistribution.get.zip(List(0.5, 0.3, 0.2)).foreach { case (a, b) =>
      assert(math.abs(a - b) < 1e-9)
    }
  }

  test("renderScaleAudit shows before/after and preserved invariants") {
    val src = sample(totalPartitions = 3000, numPartitionsToUpdate = 21)
    val scaled = WorkloadResizer.applyScale(src, ResizerConfig(
      scaleFactor = 0.01, targetPartitions = Some(300)))
    val audit = WorkloadResizer.renderScaleAudit(src, scaled,
      ResizerConfig(scaleFactor = 0.01, targetPartitions = Some(300)))
    assert(audit.contains("3000 -> 300"))
    assert(audit.contains("21 -> 3"))
    assert(audit.contains("updateRatio=0.3"))
    assert(audit.contains("zipfShape=2.0"))
    assert(audit.contains("scale factor: 0.01"))
  }
}
