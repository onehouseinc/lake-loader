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

import ai.onehouse.lakeloader.WorkloadSynthesizer.{DerivedConfig, InferredColumnCount, SchemaChoice, SuppliedSchema}
import ai.onehouse.lakeloader.configs.{KeyTypes, ResizerConfig, UpdatePatterns}
import ai.onehouse.lakeloader.parser.WorkloadResizerParser
import ai.onehouse.lakeloader.utils.ScaleTransform

import java.io.{File, PrintWriter}
import scala.io.Source

/**
 * Second-stage tool. Reads the machine-readable output of WorkloadSynthesizer
 * (synth-derived.json) and emits a scaled-down (or scaled-up) benchmark
 * configuration for ChangeDataGenerator.
 *
 * Two independent knobs:
 *  - `--scale-factor`: multiplies each per-round record count. Preserves the
 *    number of rounds (temporal cadence). e.g. 0.01 → 1% of the original volume.
 *  - `--target-partitions`: overrides total partition count. When smaller than
 *    the source, the partition-distribution vector is truncated and re-normalized;
 *    when larger, the fitted zipf shape is extrapolated. --num-partitions-to-update
 *    is preserved as a fraction of totalPartitions.
 *
 * Invariants (never rescaled): updateRatio, updatePattern, zipfianShape,
 * primary-key-type, recordSize, targetDataFileSize, schema choice.
 */
object WorkloadResizer {

  def main(args: Array[String]): Unit = {
    WorkloadResizerParser.parser.parse(args, ResizerConfig()) match {
      case Some(config) => run(config)
      case None => sys.exit(1)
    }
  }

  private[lakeloader] def run(config: ResizerConfig): Unit = {
    require(config.inputJson.nonEmpty, "--input-json is required")
    require(config.outputDir.nonEmpty, "--output-dir is required")

    val source = parseSynthDerivedJson(readFile(config.inputJson))
    val scaled = applyScale(source, config)

    val outDir = new File(config.outputDir)
    if (!outDir.exists()) outDir.mkdirs()
    writeFile(new File(outDir, "resized-full.flags"), WorkloadSynthesizer.renderFullFlags(scaled))
    writeFile(new File(outDir, "resized-summary.flags"), WorkloadSynthesizer.renderSummaryFlags(scaled))
    writeFile(new File(outDir, "resized-audit.txt"), renderScaleAudit(source, scaled, config))
    println(s"[WorkloadResizer] Wrote resized-full.flags, resized-summary.flags, and resized-audit.txt to ${config.outputDir}")
  }

  ///////////////////////
  // Scale application
  ///////////////////////

  private[lakeloader] def applyScale(source: DerivedConfig, config: ResizerConfig): DerivedConfig = {
    require(config.scaleFactor > 0.0, s"--scale-factor must be positive, got ${config.scaleFactor}")

    val scaledRecordsPerRound =
      if (config.scaleFactor == 1.0) source.recordsPerRound
      else ScaleTransform.scaleRecordsPerRound(source.recordsPerRound, config.scaleFactor)

    val targetPartitions = config.targetPartitions.getOrElse(source.totalPartitions)
    require(targetPartitions > 0, s"target partitions must be positive, got $targetPartitions")

    val (scaledDistribution, scaledRound0) =
      if (targetPartitions == source.totalPartitions) {
        (source.partitionDistribution, source.round0PartitionDistribution)
      } else {
        val newDist = ScaleTransform.scalePartitionDistribution(
          source.partitionDistribution, targetPartitions, source.zipfShape)
        val newRound0 = source.round0PartitionDistribution.map(r0 =>
          ScaleTransform.scalePartitionDistribution(r0, targetPartitions, source.zipfShape))
        (newDist, newRound0)
      }

    val scaledPartitionsToUpdate =
      if (targetPartitions == source.totalPartitions) source.numPartitionsToUpdate
      else ScaleTransform.scaleNumPartitionsToUpdate(
        source.numPartitionsToUpdate, source.totalPartitions, targetPartitions)

    val newMedian =
      if (scaledRecordsPerRound.isEmpty) 0L
      else {
        val sorted = scaledRecordsPerRound.sorted
        val n = sorted.size
        if (n % 2 == 1) sorted(n / 2)
        else (sorted(n / 2 - 1) + sorted(n / 2)) / 2L
      }

    source.copy(
      recordsPerRound = scaledRecordsPerRound,
      medianRecordsPerRound = newMedian,
      totalPartitions = targetPartitions,
      numPartitionsToUpdate = scaledPartitionsToUpdate,
      partitionDistribution = scaledDistribution.map(x => math.round(x * 1e6) / 1e6),
      round0PartitionDistribution = scaledRound0.map(_.map(x => math.round(x * 1e6) / 1e6)),
      auditNotes = source.auditNotes ++ Seq(
        s"scaled with factor=${config.scaleFactor}",
        s"source partitions=${source.totalPartitions}, target partitions=$targetPartitions"))
  }

  private[lakeloader] def renderScaleAudit(
      source: DerivedConfig,
      scaled: DerivedConfig,
      config: ResizerConfig): String = {
    val lines = Seq(
      "# WorkloadResizer audit",
      s"input: ${config.inputJson}",
      s"scale factor: ${config.scaleFactor}",
      s"target partitions: ${config.targetPartitions.map(_.toString).getOrElse("<preserved from source>")}",
      "",
      "before / after:",
      f"  totalPartitions:       ${source.totalPartitions}%d -> ${scaled.totalPartitions}%d",
      f"  numPartitionsToUpdate: ${source.numPartitionsToUpdate}%d -> ${scaled.numPartitionsToUpdate}%d",
      f"  sum(recordsPerRound):  ${source.recordsPerRound.sum}%d -> ${scaled.recordsPerRound.sum}%d",
      f"  medianRecordsPerRound: ${source.medianRecordsPerRound}%d -> ${scaled.medianRecordsPerRound}%d",
      "",
      "invariants preserved (unchanged):",
      s"  updateRatio=${source.updateRatio}",
      s"  updatePattern=${source.updatePattern}",
      s"  zipfShape=${source.zipfShape}",
      s"  recordSize=${source.recordSize}",
      s"  targetDataFileSize=${source.targetDataFileSize}",
      s"  keyType=${source.keyType}")
    lines.mkString("\n") + "\n"
  }

  ///////////////////////
  // JSON reader
  ///////////////////////

  private def readFile(path: String): String = {
    val src = Source.fromFile(path)
    try src.getLines().mkString("\n") finally src.close()
  }

  private def writeFile(f: File, content: String): Unit = {
    val pw = new PrintWriter(f)
    try pw.write(content) finally pw.close()
  }

  /**
   * Small JSON reader specialized for the flat, well-known shape of
   * synth-derived.json. Not a general-purpose parser — expects the exact
   * key set emitted by `WorkloadSynthesizer.renderDerivedJson`.
   */
  private[lakeloader] def parseSynthDerivedJson(json: String): DerivedConfig = {
    def stringVal(key: String): Option[String] = {
      val re = ("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"").r
      re.findFirstMatchIn(json).map(_.group(1).replace("\\\"", "\"").replace("\\\\", "\\"))
    }
    def numVal(key: String): Option[String] = {
      val re = ("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*(-?[0-9][0-9.eE+\\-]*)").r
      re.findFirstMatchIn(json).map(_.group(1))
    }
    def longVal(key: String): Long = numVal(key).map(_.toLong).getOrElse(
      throw new IllegalArgumentException(s"missing required numeric field: $key"))
    def intVal(key: String): Int = longVal(key).toInt
    def doubleVal(key: String): Double = numVal(key).map(_.toDouble).getOrElse(
      throw new IllegalArgumentException(s"missing required numeric field: $key"))
    def listVal(key: String): Option[String] = {
      val re = ("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*(\\[[^\\]]*\\]|null)").r
      re.findFirstMatchIn(json).map(_.group(1))
    }
    def parseLongList(raw: String): List[Long] =
      if (raw == "[]" || raw == "null") Nil
      else raw.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.toLong).toList
    def parseDoubleList(raw: String): List[Double] =
      if (raw == "[]" || raw == "null") Nil
      else raw.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.toDouble).toList

    val schemaChoice: SchemaChoice = {
      val kindRe = "\"schemaChoice\"\\s*:\\s*\\{\\s*\"kind\"\\s*:\\s*\"([^\"]+)\"".r
      val kind = kindRe.findFirstMatchIn(json)
        .map(_.group(1))
        .getOrElse(throw new IllegalArgumentException("missing schemaChoice.kind"))
      kind match {
        case "SuppliedSchema" =>
          val pathRe = "\"schemaChoice\"\\s*:\\s*\\{[^}]*\"path\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"".r
          val p = pathRe.findFirstMatchIn(json).map(_.group(1))
            .getOrElse(throw new IllegalArgumentException("SuppliedSchema missing path"))
          SuppliedSchema(p)
        case "InferredColumnCount" =>
          val nRe = "\"schemaChoice\"\\s*:\\s*\\{[^}]*\"numColumns\"\\s*:\\s*(-?\\d+)".r
          val n = nRe.findFirstMatchIn(json).map(_.group(1).toInt)
            .getOrElse(throw new IllegalArgumentException("InferredColumnCount missing numColumns"))
          InferredColumnCount(n)
        case other => throw new IllegalArgumentException(s"unknown schemaChoice.kind: $other")
      }
    }

    // commitStats is an array of flat objects — extract the array body, split into
    // per-entry object strings, then pull scalar fields from each. Tolerates any
    // whitespace between entries but assumes no nested brackets.
    val commitStats: List[WorkloadSynthesizer.CommitStat] = {
      val re = "\"commitStats\"\\s*:\\s*\\[([\\s\\S]*?)\\]".r
      re.findFirstMatchIn(json) match {
        case None => Nil
        case Some(m) =>
          val body = m.group(1).trim
          if (body.isEmpty) Nil
          else {
            // Split on the boundary between adjacent objects. Each entry starts with { and ends with }.
            val entries = body.split("\\}\\s*,\\s*\\{").toList
            entries.map { rawIn =>
              val raw = "{" + rawIn.stripPrefix("{").stripSuffix("}") + "}"
              def strField(k: String): String = {
                val fre = ("\"" + java.util.regex.Pattern.quote(k) + "\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"").r
                fre.findFirstMatchIn(raw).map(_.group(1))
                  .getOrElse(throw new IllegalArgumentException(s"commitStat missing string field: $k"))
              }
              def numField(k: String): String = {
                val fre = ("\"" + java.util.regex.Pattern.quote(k) + "\"\\s*:\\s*(-?[0-9][0-9.eE+\\-]*)").r
                fre.findFirstMatchIn(raw).map(_.group(1))
                  .getOrElse(throw new IllegalArgumentException(s"commitStat missing numeric field: $k"))
              }
              WorkloadSynthesizer.CommitStat(
                instantTime = strField("instantTime"),
                inserts = numField("inserts").toLong,
                updates = numField("updates").toLong,
                numPartitionsWithInserts = numField("numPartitionsWithInserts").toInt,
                numPartitionsWithUpdates = numField("numPartitionsWithUpdates").toInt,
                insertZipfShape = numField("insertZipfShape").toDouble)
            }
          }
      }
    }

    val recordsPerRound = listVal("recordsPerRound").map(parseLongList).getOrElse(Nil)
    val partitionDistribution = listVal("partitionDistribution").map(parseDoubleList).getOrElse(Nil)
    val round0PartitionDistribution: Option[List[Double]] =
      listVal("round0PartitionDistribution") match {
        case Some("null") => None
        case Some(raw) => Some(parseDoubleList(raw))
        case None => None
      }

    val keyTypeStr = stringVal("keyType").getOrElse("Random")
    val keyType = KeyTypes.withName(keyTypeStr)
    val updatePatternStr = stringVal("updatePattern").getOrElse("Uniform")
    val updatePattern = UpdatePatterns.withName(updatePatternStr)

    DerivedConfig(
      numRounds = intVal("numRounds"),
      recordsPerRound = recordsPerRound,
      medianRecordsPerRound = longVal("medianRecordsPerRound"),
      totalPartitions = intVal("totalPartitions"),
      updateRatio = doubleVal("updateRatio"),
      numPartitionsToUpdate = intVal("numPartitionsToUpdate"),
      recordSize = intVal("recordSize"),
      targetDataFileSize = intVal("targetDataFileSize"),
      updatePattern = updatePattern,
      zipfShape = doubleVal("zipfShape"),
      partitionDistribution = partitionDistribution,
      round0PartitionDistribution = round0PartitionDistribution,
      keyType = keyType,
      keyTypeSource = stringVal("keyTypeSource").getOrElse("unknown"),
      recordKeyField = stringVal("recordKeyField"),
      schemaChoice = schemaChoice,
      commitStats = commitStats,
      auditNotes = Seq(s"source: ${stringVal("sourceTablePath").getOrElse("<unknown>")}"))
  }
}
