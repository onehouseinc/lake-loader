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

import ai.onehouse.lakeloader.WorkloadSynthesizer.CommitAgg
import ai.onehouse.lakeloader.configs.{DatagenConfig, KeyTypes, SynthesizerConfig, UpdatePatterns}
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser
import org.scalatest.funsuite.AnyFunSuite

class WorkloadSynthesizerSpec extends AnyFunSuite {

  private def commit(
      instant: String,
      inserts: Map[String, Long] = Map.empty,
      updates: Map[String, Long] = Map.empty,
      freshFileSizes: Seq[Long] = Seq(128L * 1024L * 1024L),
      recordSizeBytes: Long = 512L): CommitAgg = {
    val insTotal = inserts.values.sum
    val updTotal = updates.values.sum
    CommitAgg(
      instant = instant,
      action = "commit",
      inserts = insTotal,
      updates = updTotal,
      bytesWritten = (insTotal + updTotal) * recordSizeBytes,
      partitionInserts = inserts,
      partitionUpdates = updates,
      freshFileSizes = freshFileSizes)
  }

  private val defaultConfig = SynthesizerConfig(
    tablePath = "/dummy",
    outputDir = "/dummy",
    minZipfShapeToEmit = 0.3)

  test("deriveConfig extracts record counts, update ratio, partitions") {
    val commits = List(
      commit("t0", inserts = Map("a" -> 1000L, "b" -> 500L)),
      commit("t1", inserts = Map("a" -> 800L, "b" -> 200L), updates = Map("a" -> 300L)),
      commit("t2", inserts = Map("a" -> 900L), updates = Map("a" -> 400L, "b" -> 100L)))

    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)

    assert(d.numRounds == 3)
    assert(d.recordsPerRound == List(1500L, 1300L, 1400L))
    assert(d.totalPartitions == 2)
    // per-commit update ratios: 0, 300/1300, 500/1400 → mean ≈ 0.196
    assert(math.abs(d.updateRatio - 0.196) < 0.01, s"got ${d.updateRatio}")
    // partitions with updates per commit: 0, 1, 2 → median of non-empty = 1.5 → rounded 2
    assert(d.numPartitionsToUpdate == 2)
    // bytes/record = 512
    assert(d.recordSize == 512)
  }

  test("deriveConfig picks Zipf when inserts are skewed") {
    // insert counts across 10 partitions follow ~1/rank^2 (shape=2)
    val perPartition: Map[String, Long] = (1 to 10).map { r =>
      s"p$r" -> math.max(1L, (100000.0 / math.pow(r, 2)).toLong)
    }.toMap
    val commits = List(commit("t0", inserts = perPartition), commit("t1", inserts = perPartition))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)

    assert(d.updatePattern == UpdatePatterns.Zipf, s"got ${d.updatePattern}")
    assert(math.abs(d.zipfShape - 2.0) < 0.15, s"got ${d.zipfShape}")
    assert(d.partitionDistribution.head > 0.5, s"head weight ${d.partitionDistribution.head} should dominate")
  }

  test("deriveConfig picks Uniform when inserts are flat") {
    val perPartition = (1 to 10).map(r => s"p$r" -> 1000L).toMap
    val commits = List(commit("t0", inserts = perPartition), commit("t1", inserts = perPartition))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)

    assert(d.updatePattern == UpdatePatterns.Uniform)
    assert(d.zipfShape == 0.0)
  }

  test("deriveConfig detects distinct round-0 partition distribution") {
    // round 0 hits all partitions uniformly; subsequent rounds concentrate on p1..p3
    val round0 = (1 to 20).map(r => s"p$r" -> 100L).toMap
    val laterRounds = Map("p1" -> 800L, "p2" -> 150L, "p3" -> 50L)
    val commits = List(
      commit("t0", inserts = round0),
      commit("t1", inserts = laterRounds),
      commit("t2", inserts = laterRounds))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)

    assert(d.round0PartitionDistribution.isDefined)
  }

  test("renderFullFlags produces a parser-consumable flag string") {
    val commits = List(
      commit("t0", inserts = Map("a" -> 1000L, "b" -> 500L)),
      commit("t1", inserts = Map("a" -> 800L), updates = Map("a" -> 200L)))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)

    val out = WorkloadSynthesizer.renderFullFlags(d)

    // Replace the placeholder path + schema with real dummy values before feeding to the parser.
    val runnable = out
      .replace("<fill-in>", "/tmp/out")
      .replace("<fill-in>.avsc", "/tmp/schema.avsc")

    val args = runnable.trim.split("\\s+").map(_.replaceAll("^'|'$", ""))
    val parsed = ChangeDataGeneratorParser.parser.parse(args, DatagenConfig())
    assert(parsed.isDefined, s"parser rejected emitted flags:\n$out")

    val cfg = parsed.get
    assert(cfg.numberOfRounds == 2)
    assert(cfg.roundsDistribution == List(1500L, 1000L))
    assert(cfg.totalPartitions == 2)
  }

  test("renderSummaryFlags collapses per-round counts to median") {
    val commits = (1 to 5).map(i => commit(s"t$i", inserts = Map("a" -> (i * 1000L)))).toList
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)

    val out = WorkloadSynthesizer.renderSummaryFlags(d)
    assert(out.contains("--number-records-per-round 3000"), s"expected median=3000 in:\n$out")
    assert(out.contains("--number-rounds 5"))
  }

  test("renderFullFlags emits --zipfian-shape only when pattern is Zipf") {
    val flat = (1 to 5).map(r => s"p$r" -> 1000L).toMap
    val flatCommits = List(commit("t0", inserts = flat), commit("t1", inserts = flat))
    val dFlat = WorkloadSynthesizer.deriveConfig(
      flatCommits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)
    val outFlat = WorkloadSynthesizer.renderFullFlags(dFlat)
    assert(!outFlat.contains("--zipfian-shape"), s"unexpected zipf flag on uniform:\n$outFlat")

    val skewed: Map[String, Long] = (1 to 10).map { r =>
      s"p$r" -> math.max(1L, (100000.0 / math.pow(r, 2.5)).toLong)
    }.toMap
    val skewedCommits = List(commit("t0", inserts = skewed), commit("t1", inserts = skewed))
    val dSkewed = WorkloadSynthesizer.deriveConfig(
      skewedCommits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)
    val outSkewed = WorkloadSynthesizer.renderFullFlags(dSkewed)
    assert(outSkewed.contains("--zipfian-shape"), s"missing zipf flag on skewed:\n$outSkewed")
  }

  test("renderFullFlags emits two-segment --partition-distribution when round 0 differs") {
    val round0 = (1 to 20).map(r => s"p$r" -> 100L).toMap
    val laterRounds = Map("p1" -> 800L, "p2" -> 150L, "p3" -> 50L)
    val commits = List(
      commit("t0", inserts = round0),
      commit("t1", inserts = laterRounds),
      commit("t2", inserts = laterRounds))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), Seq.empty)

    val out = WorkloadSynthesizer.renderFullFlags(d)
    val partLine = out.split("\n").find(_.startsWith("--partition-distribution")).getOrElse("")
    assert(partLine.contains(";"), s"expected two-segment form in:\n$partLine")
  }

  test("renderAudit contains derived values and notes") {
    val commits = List(commit("t0", inserts = Map("a" -> 100L)))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "cli-override", Some("id"), Seq("note-one"))
    val audit = WorkloadSynthesizer.renderAudit(d, "s3://bucket/table")
    assert(audit.contains("source table: s3://bucket/table"))
    assert(audit.contains("key type source: cli-override"))
    assert(audit.contains("record key field: id"))
    assert(audit.contains("note-one"))
  }
}
