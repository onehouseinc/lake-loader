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

import ai.onehouse.lakeloader.WorkloadSynthesizer.{CommitAgg, FooterSample, InferredColumnCount, SuppliedSchema}
import ai.onehouse.lakeloader.configs.{DatagenConfig, KeyTypes, SynthesizerConfig, UpdatePatterns}
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class WorkloadSynthesizerSpec extends AnyFunSuite {

  private val defaultSchema = InferredColumnCount(10)
  private val emptyPartitionSize = WorkloadSynthesizer.PartitionSizeStats(0, Nil, 0L)

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
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)

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
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)

    assert(d.updatePattern == UpdatePatterns.Zipf, s"got ${d.updatePattern}")
    assert(math.abs(d.zipfShape - 2.0) < 0.15, s"got ${d.zipfShape}")
    assert(d.partitionDistribution.head > 0.5, s"head weight ${d.partitionDistribution.head} should dominate")
  }

  test("deriveConfig picks Uniform when inserts are flat") {
    val perPartition = (1 to 10).map(r => s"p$r" -> 1000L).toMap
    val commits = List(commit("t0", inserts = perPartition), commit("t1", inserts = perPartition))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)

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
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)

    assert(d.round0PartitionDistribution.isDefined)
  }

  test("renderFullFlags produces a parser-consumable flag string") {
    val commits = List(
      commit("t0", inserts = Map("a" -> 1000L, "b" -> 500L)),
      commit("t1", inserts = Map("a" -> 800L), updates = Map("a" -> 200L)))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)

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
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)

    val out = WorkloadSynthesizer.renderSummaryFlags(d)
    assert(out.contains("--number-records-per-round 3000"), s"expected median=3000 in:\n$out")
    assert(out.contains("--number-rounds 5"))
  }

  test("renderFullFlags emits --zipfian-shape only when pattern is Zipf") {
    val flat = (1 to 5).map(r => s"p$r" -> 1000L).toMap
    val flatCommits = List(commit("t0", inserts = flat), commit("t1", inserts = flat))
    val dFlat = WorkloadSynthesizer.deriveConfig(
      flatCommits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)
    val outFlat = WorkloadSynthesizer.renderFullFlags(dFlat)
    assert(!outFlat.contains("--zipfian-shape"), s"unexpected zipf flag on uniform:\n$outFlat")

    val skewed: Map[String, Long] = (1 to 10).map { r =>
      s"p$r" -> math.max(1L, (100000.0 / math.pow(r, 2.5)).toLong)
    }.toMap
    val skewedCommits = List(commit("t0", inserts = skewed), commit("t1", inserts = skewed))
    val dSkewed = WorkloadSynthesizer.deriveConfig(
      skewedCommits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)
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
      commits, defaultConfig, KeyTypes.Random, "test", Some("key"), defaultSchema, emptyPartitionSize, Seq.empty)

    val out = WorkloadSynthesizer.renderFullFlags(d)
    val partLine = out.split("\n").find(_.startsWith("--partition-distribution")).getOrElse("")
    assert(partLine.contains(";"), s"expected two-segment form in:\n$partLine")
  }

  test("renderAudit contains derived values and notes") {
    val commits = List(commit("t0", inserts = Map("a" -> 100L)))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "cli-override", Some("id"), defaultSchema, emptyPartitionSize, Seq("note-one"))
    val audit = WorkloadSynthesizer.renderAudit(d, "s3://bucket/table")
    assert(audit.contains("source table: s3://bucket/table"))
    assert(audit.contains("key type source: cli-override"))
    assert(audit.contains("record key field: id"))
    assert(audit.contains("note-one"))
    assert(audit.contains("schemaChoice=InferredColumnCount(numColumns=10)"))
  }

  test("renderFullFlags emits --number-columns for InferredColumnCount schema") {
    val commits = List(commit("t0", inserts = Map("a" -> 100L)))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("id"),
      InferredColumnCount(17), emptyPartitionSize, Seq.empty)
    val out = WorkloadSynthesizer.renderFullFlags(d)
    assert(out.contains("--number-columns 17"), s"expected --number-columns 17 in:\n$out")
    assert(!out.contains("--avro-schema"), s"should not emit --avro-schema for InferredColumnCount:\n$out")
  }

  test("renderFullFlags emits --avro-schema for SuppliedSchema") {
    val commits = List(commit("t0", inserts = Map("a" -> 100L)))
    val d = WorkloadSynthesizer.deriveConfig(
      commits, defaultConfig, KeyTypes.Random, "test", Some("id"),
      SuppliedSchema("/path/to/schema.avsc"), emptyPartitionSize, Seq.empty)
    val out = WorkloadSynthesizer.renderFullFlags(d)
    assert(out.contains("--avro-schema /path/to/schema.avsc"),
      s"expected --avro-schema in:\n$out")
    assert(!out.contains("--number-columns"),
      s"should not emit --number-columns for SuppliedSchema:\n$out")
  }

  test("anonymizeAvroSchema rewrites top-level field names by type") {
    import org.apache.avro.Schema
    val json =
      """{
        |  "type": "record",
        |  "name": "Customer",
        |  "namespace": "com.example",
        |  "fields": [
        |    {"name": "customer_id", "type": "long"},
        |    {"name": "email_address", "type": "string"},
        |    {"name": "signup_ts", "type": "long"},
        |    {"name": "is_premium", "type": "boolean"},
        |    {"name": "score", "type": "double"}
        |  ]
        |}""".stripMargin
    val original = new Schema.Parser().parse(json)
    val anon = WorkloadSynthesizer.anonymizeAvroSchema(original)
    val names = anon.getFields.asScala.map(_.name()).toList
    assert(names == List("col_long_a", "col_string_b", "col_long_c", "col_bool_d", "col_double_e"),
      s"unexpected names: $names")
    // Types preserved
    val types = anon.getFields.asScala.map(_.schema().getType).toList
    assert(types == List(Schema.Type.LONG, Schema.Type.STRING, Schema.Type.LONG,
      Schema.Type.BOOLEAN, Schema.Type.DOUBLE))
    // No sensitive names leaked
    assert(!anon.toString.contains("customer_id"))
    assert(!anon.toString.contains("email_address"))
    assert(!anon.toString.contains("Customer"))
  }

  test("anonymizeAvroSchema handles nullable (union) fields") {
    import org.apache.avro.Schema
    val json =
      """{
        |  "type": "record",
        |  "name": "Rec",
        |  "fields": [
        |    {"name": "maybe_str", "type": ["null", "string"], "default": null}
        |  ]
        |}""".stripMargin
    val original = new Schema.Parser().parse(json)
    val anon = WorkloadSynthesizer.anonymizeAvroSchema(original)
    val f = anon.getFields.asScala.head
    assert(f.name() == "col_string_a")
    // The union is preserved intact so lake-loader still generates nullable values.
    assert(f.schema().getType == Schema.Type.UNION)
  }

  test("anonymizeAvroSchema recursively renames nested record fields") {
    import org.apache.avro.Schema
    val json =
      """{
        |  "type": "record",
        |  "name": "Outer",
        |  "fields": [
        |    {"name": "user_name", "type": "string"},
        |    {"name": "address", "type": {
        |      "type": "record",
        |      "name": "Address",
        |      "fields": [
        |        {"name": "street_line_1", "type": "string"},
        |        {"name": "zip", "type": "int"}
        |      ]
        |    }}
        |  ]
        |}""".stripMargin
    val original = new Schema.Parser().parse(json)
    val anon = WorkloadSynthesizer.anonymizeAvroSchema(original)
    assert(anon.getFields.asScala.map(_.name()).toList == List("col_string_a", "col_record_b"))
    val nested = anon.getFields.asScala.find(_.name() == "col_record_b").get.schema()
    assert(nested.getFields.asScala.map(_.name()).toList == List("col_string_a", "col_int_b"))
    // Sensitive nested names removed
    assert(!anon.toString.contains("street_line_1"))
    assert(!anon.toString.contains("user_name"))
  }

  ///////////////////////
  // Footer-based key-type inference
  ///////////////////////

  test("extractInstantFromFileName parses Hudi-style base file names") {
    // <fileId>_<writeToken>_<instantTime>.parquet
    val name = "s3://bucket/table/2025-01-01/e3c9-1_0-0-0_20250101120000.parquet"
    assert(WorkloadSynthesizer.extractInstantFromFileName(name) == "20250101120000")
  }

  test("extractInstantFromFileName returns empty when name doesn't match Hudi pattern") {
    assert(WorkloadSynthesizer.extractInstantFromFileName("/path/to/data.parquet") == "")
  }

  test("spearmanRankCorrelation returns ~1.0 for monotonic input") {
    val instants = Seq("20250101", "20250102", "20250103", "20250104", "20250105")
    val mins = Seq("a", "b", "c", "d", "e")
    val corr = WorkloadSynthesizer.spearmanRankCorrelation(instants, mins)
    assert(math.abs(corr - 1.0) < 1e-9, s"expected ~1.0, got $corr")
  }

  test("spearmanRankCorrelation returns ~-1.0 for reversed input") {
    val instants = Seq("20250101", "20250102", "20250103", "20250104")
    val mins = Seq("d", "c", "b", "a")
    val corr = WorkloadSynthesizer.spearmanRankCorrelation(instants, mins)
    assert(math.abs(corr + 1.0) < 1e-9, s"expected ~-1.0, got $corr")
  }

  test("spearmanRankCorrelation returns ~0 for uncorrelated input") {
    // instants monotonic; mins scrambled — should be near 0
    val instants = Seq("t1", "t2", "t3", "t4", "t5", "t6")
    val mins = Seq("c", "a", "e", "b", "f", "d")
    val corr = WorkloadSynthesizer.spearmanRankCorrelation(instants, mins)
    assert(math.abs(corr) < 0.6, s"expected small |corr|, got $corr")
  }

  test("spearmanRankCorrelation returns 0 for n<2") {
    assert(WorkloadSynthesizer.spearmanRankCorrelation(Seq("a"), Seq("b")) == 0.0)
    assert(WorkloadSynthesizer.spearmanRankCorrelation(Seq.empty, Seq.empty) == 0.0)
  }

  test("classifyFromFooterStats: UUID-shaped random keys → Random") {
    // Every file has min starting with a low hex char and max starting with a high hex char
    val samples = (1 to 10).map { i =>
      FooterSample(
        path = s"/f$i.parquet",
        instantTime = s"2025010${i}",
        min = Some(f"0${i}${randomHexTail(6)}"),
        max = Some(f"f${i}${randomHexTail(6)}"))
    }.toList
    val (kt, source, notes) = WorkloadSynthesizer.classifyFromFooterStats(samples)
    assert(kt == KeyTypes.Random, s"expected Random, got $kt")
    assert(source.contains("uuid") || source.contains("random"), s"got $source")
  }

  test("classifyFromFooterStats: monotonic epoch-prefix → TemporallyOrdered") {
    // Each file's min and max grow monotonically with instant
    val samples = (1 to 10).map { i =>
      FooterSample(
        path = s"/f$i.parquet",
        instantTime = f"2025010${i}%d",
        min = Some(f"170000${i}%d"),
        max = Some(f"170000${i}%d"))
    }.toList
    val (kt, _, notes) = WorkloadSynthesizer.classifyFromFooterStats(samples)
    assert(kt == KeyTypes.TemporallyOrdered, s"expected TemporallyOrdered, got $kt")
  }

  test("classifyFromFooterStats: hybrid (temporal + random suffix) → TemporallyOrdered with hybrid note") {
    // Snowflake-ID style: leading char encodes a coarse timestamp bucket (drifts
    // upward with instant time), with a random suffix. Within one file, min and
    // max have different leading chars (wide range), but the file's overall
    // min-value ordering correlates with instant time — because the leading
    // bucket char shifts. Min head does NOT saturate '0' (so not UUID-shaped).
    val minHeads = "34567".toIndexedSeq // never '0'..'3', so hexShapeRatio never saturates
    val maxHeads = "89abc".toIndexedSeq // ends short of 'f', but max head still > min head
    val samples = (0 until 10).map { i =>
      val bucket = i / 2 // 0..4, growing with time
      FooterSample(
        path = s"/f$i.parquet",
        instantTime = f"2025010${i}%d",
        min = Some(s"${minHeads(bucket)}${randomHexTail(6)}"),
        max = Some(s"${maxHeads(bucket)}${randomHexTail(6)}"))
    }.toList
    val (kt, source, notes) = WorkloadSynthesizer.classifyFromFooterStats(samples)
    assert(kt == KeyTypes.TemporallyOrdered, s"expected TemporallyOrdered for hybrid, got $kt")
    assert(source == "footer-stats-hybrid-temporal-random",
      s"got source=$source, notes=$notes")
  }

  test("classifyFromFooterStats: fewer than 3 usable samples → Random with insufficient note") {
    val samples = List(
      FooterSample("/f1.parquet", "1", Some("a"), Some("z")),
      FooterSample("/f2.parquet", "2", None, None))
    val (kt, source, notes) = WorkloadSynthesizer.classifyFromFooterStats(samples)
    assert(kt == KeyTypes.Random)
    assert(source == "insufficient-footer-stats")
  }

  private def randomHexTail(len: Int): String = {
    val r = new scala.util.Random(42)
    val chars = "0123456789abcdef"
    (1 to len).map(_ => chars(r.nextInt(chars.length))).mkString
  }
}
