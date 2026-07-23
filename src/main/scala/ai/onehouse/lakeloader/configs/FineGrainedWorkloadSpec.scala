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

package ai.onehouse.lakeloader.configs

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.{BufferedReader, InputStreamReader}
import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Exact insert/update counts for one partition within one commit.
 */
case class PartitionOps(inserts: Long, updates: Long)

/**
 * One incremental commit: exact per-partition operation counts, only for the partitions
 * touched by this commit. Partition values are date strings in `yyyy-MM-dd` format,
 * kept sorted ascending for deterministic generation.
 */
case class CommitSpec(partitionOps: List[(String, PartitionOps)])

/**
 * Bootstrap (round 0) definition: `totalRecords` inserts distributed evenly across one
 * partition per day in `[startDate, endDate]` (both inclusive).
 */
case class BootstrapSpec(startDate: LocalDate, endDate: LocalDate, totalRecords: Long) {

  def numPartitions: Int = (ChronoUnit.DAYS.between(startDate, endDate) + 1).toInt

  /** Partition values in ascending date order, formatted `yyyy-MM-dd`. */
  def partitionValues: List[String] =
    (0 until numPartitions).map(d => startDate.plusDays(d.toLong).toString).toList
}

/**
 * Fine-grained workload spec: a bootstrap round followed by N commits, each prescribing
 * exact insert/update counts per touched partition. Parsed from a JSON file of the form:
 *
 * {{{
 * {
 *   "bootstrap": {"startDate": "2026-01-01", "endDate": "2026-03-31", "totalRecords": 100000000},
 *   "commits": [
 *     {"2026-01-01": {"inserts": 1000, "updates": 500}, "2026-01-02": {"inserts": 2000}},
 *     {"2026-02-01": {"updates": 5000}}
 *   ]
 * }
 * }}}
 *
 * Round numbering: round 0 = bootstrap, round k = commits(k - 1).
 */
case class FineGrainedWorkloadSpec(bootstrap: BootstrapSpec, commits: List[CommitSpec]) {
  def totalRounds: Int = 1 + commits.size
}

object FineGrainedWorkloadSpec {

  private val MAX_BOOTSTRAP_PARTITIONS = 1000000L

  /**
   * Load and validate a workload spec from a Hadoop-compatible path (local, s3, hdfs, ...).
   */
  def fromJsonFile(path: String, hadoopConf: Configuration): FineGrainedWorkloadSpec = {
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(hadoopConf)
    val inputStream = fs.open(fsPath)
    try {
      val content = new BufferedReader(new InputStreamReader(inputStream))
        .lines()
        .collect(Collectors.joining("\n"))
      fromJsonString(content)
    } finally {
      inputStream.close()
    }
  }

  /**
   * Parse and validate a workload spec from a JSON string. Throws [[IllegalArgumentException]]
   * with a descriptive message on any malformed or semantically invalid input.
   */
  def fromJsonString(json: String): FineGrainedWorkloadSpec = {
    val root =
      try {
        new ObjectMapper().readTree(json)
      } catch {
        case e: JsonProcessingException =>
          throw new IllegalArgumentException(s"Workload spec is not valid JSON: ${e.getMessage}", e)
      }
    require(root != null && root.isObject, "Workload spec must be a JSON object")
    checkAllowedFields(root, Set("bootstrap", "commits"), "workload spec")

    val spec = FineGrainedWorkloadSpec(parseBootstrap(root), parseCommits(root))
    validateUpdateTargets(spec)
    spec
  }

  private def parseBootstrap(root: JsonNode): BootstrapSpec = {
    val node = root.get("bootstrap")
    require(node != null && node.isObject, "Workload spec must have a 'bootstrap' object")
    checkAllowedFields(node, Set("startDate", "endDate", "totalRecords"), "'bootstrap'")

    val startDate = parseDate(requiredText(node, "startDate", "bootstrap"), "bootstrap.startDate")
    val endDate = parseDate(requiredText(node, "endDate", "bootstrap"), "bootstrap.endDate")
    require(
      !startDate.isAfter(endDate),
      s"bootstrap.startDate ($startDate) must not be after bootstrap.endDate ($endDate)")
    val numPartitions = ChronoUnit.DAYS.between(startDate, endDate) + 1
    require(
      numPartitions <= MAX_BOOTSTRAP_PARTITIONS,
      s"Bootstrap date range spans $numPartitions daily partitions, exceeding the sanity limit " +
        s"of $MAX_BOOTSTRAP_PARTITIONS")

    val totalRecords = requiredLong(node, "totalRecords", "bootstrap")
    require(totalRecords > 0, s"bootstrap.totalRecords must be > 0, got $totalRecords")
    BootstrapSpec(startDate, endDate, totalRecords)
  }

  private def parseCommits(root: JsonNode): List[CommitSpec] = {
    val node = root.get("commits")
    if (node == null || node.isNull) {
      List.empty
    } else {
      require(node.isArray, "'commits' must be a JSON array")
      node
        .elements()
        .asScala
        .zipWithIndex
        .map { case (commitNode, idx) =>
          parseCommit(commitNode, commitNumber = idx + 1)
        }
        .toList
    }
  }

  private def parseCommit(commitNode: JsonNode, commitNumber: Int): CommitSpec = {
    require(
      commitNode.isObject && commitNode.size() > 0,
      s"Commit #$commitNumber must be a non-empty JSON object mapping 'yyyy-MM-dd' partition " +
        "dates to {inserts, updates} counts")
    val ops = commitNode
      .fields()
      .asScala
      .map { entry =>
        val date = parseDate(entry.getKey, s"commit #$commitNumber partition date")
        val opsNode = entry.getValue
        require(
          opsNode.isObject,
          s"Commit #$commitNumber partition '${entry.getKey}' must map to a JSON object " +
            "like {\"inserts\": 1000, \"updates\": 500}")
        checkAllowedFields(
          opsNode,
          Set("inserts", "updates"),
          s"commit #$commitNumber partition '${entry.getKey}'")
        val inserts =
          optionalLong(opsNode, "inserts", 0L, s"commit #$commitNumber '${entry.getKey}'")
        val updates =
          optionalLong(opsNode, "updates", 0L, s"commit #$commitNumber '${entry.getKey}'")
        require(
          inserts >= 0 && updates >= 0,
          s"Commit #$commitNumber partition '${entry.getKey}': inserts and updates must be >= 0, " +
            s"got inserts=$inserts, updates=$updates")
        require(
          inserts + updates > 0,
          s"Commit #$commitNumber partition '${entry.getKey}': at least one of inserts/updates " +
            "must be > 0 (drop the partition entry instead)")
        (date, PartitionOps(inserts, updates))
      }
      .toList
      .sortBy(_._1.toEpochDay)
    CommitSpec(ops.map { case (date, partitionOps) => (date.toString, partitionOps) })
  }

  /**
   * Updates can only target partitions that already hold data when the commit runs: a date
   * within the bootstrap range, or a date some *earlier* commit inserted into. A commit that
   * both opens a new partition and updates it is rejected — its own inserts are not visible
   * to its updates (updates are sampled from previously generated rounds only).
   */
  private def validateUpdateTargets(spec: FineGrainedWorkloadSpec): Unit = {
    val insertedDates = mutable.Set[LocalDate]()
    spec.commits.zipWithIndex.foreach { case (commit, idx) =>
      val commitNumber = idx + 1
      commit.partitionOps.foreach { case (dateStr, ops) =>
        if (ops.updates > 0) {
          val date = LocalDate.parse(dateStr)
          val inBootstrapRange =
            !date.isBefore(spec.bootstrap.startDate) && !date.isAfter(spec.bootstrap.endDate)
          require(
            inBootstrapRange || insertedDates.contains(date),
            s"Commit #$commitNumber requests updates for partition '$dateStr', but that " +
              "partition has no data: it is outside the bootstrap date range and no earlier " +
              "commit inserts into it")
        }
      }
      commit.partitionOps.foreach { case (dateStr, ops) =>
        if (ops.inserts > 0) {
          insertedDates += LocalDate.parse(dateStr)
        }
      }
    }
  }

  private def checkAllowedFields(node: JsonNode, allowed: Set[String], where: String): Unit = {
    node.fieldNames().asScala.foreach { name =>
      require(
        allowed.contains(name),
        s"Unknown field '$name' in $where; allowed fields: ${allowed.toList.sorted.mkString(", ")}")
    }
  }

  private def requiredText(node: JsonNode, field: String, where: String): String = {
    val value = node.get(field)
    require(value != null && value.isTextual, s"'$where.$field' is required and must be a string")
    value.asText()
  }

  private def requiredLong(node: JsonNode, field: String, where: String): Long = {
    val value = node.get(field)
    require(
      value != null && value.canConvertToLong && value.isIntegralNumber,
      s"'$where.$field' is required and must be an integer")
    value.asLong()
  }

  private def optionalLong(node: JsonNode, field: String, default: Long, where: String): Long = {
    val value = node.get(field)
    if (value == null || value.isNull) {
      default
    } else {
      require(
        value.canConvertToLong && value.isIntegralNumber,
        s"'$field' in $where must be an integer")
      value.asLong()
    }
  }

  private def parseDate(raw: String, what: String): LocalDate = {
    try {
      LocalDate.parse(raw.trim)
    } catch {
      case _: DateTimeParseException =>
        throw new IllegalArgumentException(s"$what: '$raw' is not a valid yyyy-MM-dd date")
    }
  }
}
