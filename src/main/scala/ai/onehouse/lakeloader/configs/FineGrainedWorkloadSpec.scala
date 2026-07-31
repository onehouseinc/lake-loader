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

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
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
 *
 * @param suffixKeyWithPartitionPath when true, round-0 keys get `_<partitionPath>` appended, i.e.
 *                                   `<uuid>-<round>_<partition>`. Set this when the bootstrap is a
 *                                   single seed partition that will later be fanned out across
 *                                   many partitions by copying its base files and rewriting only
 *                                   the key suffix (the `ParquetPartitionRewriteJob` flow): that
 *                                   rewriter splits on the *last* underscore, so a key without one
 *                                   gains a suffix rather than having it replaced. Like
 *                                   [[ExternalBootstrapSpec.suffixKeyWithPartitionPath]], seed and
 *                                   incremental insert keys then both end in `_<partition>` with a
 *                                   single underscore; unlike it, the `%03d` round tag is kept
 *                                   here, so round-0 keys stay self-identifying after the fan-out.
 */
case class BootstrapSpec(
    startDate: LocalDate,
    endDate: LocalDate,
    totalRecords: Long,
    suffixKeyWithPartitionPath: Boolean = false) {

  def numPartitions: Int = (ChronoUnit.DAYS.between(startDate, endDate) + 1).toInt

  /** Partition values in ascending date order, formatted `yyyy-MM-dd`. */
  def partitionValues: List[String] =
    (0 until numPartitions).map(d => startDate.plusDays(d.toLong).toString).toList
}

/**
 * Opt-in alternative to `bootstrap`: skip round-0 generation entirely and instead treat an
 * already-populated Hudi table as the bootstrap. When set, update targets for every commit are
 * read back from this table (`recordKeyField`/`partitionPathField` columns, partition-pruned) and
 * new inserts are freshly-keyed. A single payload pool (data columns only, no identity) is
 * generated once per job, sized to `payloadPoolMultiplier` times the largest single-partition,
 * single-commit (inserts + updates) demand across the whole spec, and reused (via sampling)
 * across every commit and partition — avoiding redundant regeneration of the payload data when
 * many commits touch the same hot partitions.
 *
 * IMPORTANT constraint (unlike the normal `bootstrap` path): update targets are read from the
 * live external Hudi table *at datagen time*, not from lake-loader's own previously-generated
 * round directories. If a commit's workload spec requests updates on a partition whose only
 * records came from an *earlier lake-loader round* that has not yet actually been loaded into the
 * external table (e.g. the whole spec is datagen'd up front, before any round is loaded), those
 * keys will not be visible yet and the update request will fail. Only request updates on
 * partitions that were already populated in the external table at the time the bootstrap snapshot
 * was taken.
 *
 * @param suffixKeyWithPartitionPath when true, new-insert keys are generated as
 *                                   `<uuid>_<partitionPath>` instead of the normal
 *                                   `keyType`-based scheme. Matches a bootstrap built by
 *                                   generating real data for a single partition and copying it
 *                                   verbatim to every other partition, rewriting only the key to
 *                                   stay globally unique (`uuid_partitionPath`) -- new inserts in
 *                                   the incremental batches then follow the same convention.
 *                                   Update keys are unaffected either way: they are always reused
 *                                   verbatim from the external table.
 */
case class ExternalBootstrapSpec(
    tablePath: String,
    payloadPoolMultiplier: Double = 2.0,
    recordKeyField: String = "_hoodie_record_key",
    partitionPathField: String = "_hoodie_partition_path",
    suffixKeyWithPartitionPath: Boolean = false)

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
 * Round numbering: round 0 = bootstrap, round k = commits(k - 1). When `externalBootstrap` is set
 * instead of `bootstrap`, round 0 is never generated and round numbering starts at 1.
 *
 * Optional top-level `nullifiedPartitions`: partition values (dates) whose generated records
 * should have every field nulled out except the identity fields (key/partition/round/ts). Used
 * to replicate a real table's full partition count/RLI-shard footprint without paying the
 * on-disk cost of fully-populated records for cold/historical partitions that are never touched
 * by real incremental traffic.
 *
 * Exactly one of `bootstrap`/`externalBootstrap` must be set.
 */
case class FineGrainedWorkloadSpec(
    bootstrap: Option[BootstrapSpec],
    commits: List[CommitSpec],
    nullifiedPartitions: Set[String] = Set.empty,
    externalBootstrap: Option[ExternalBootstrapSpec] = None) {
  // Round numbering is fixed regardless of mode: commit i is always round i (1-indexed); round 0
  // is the bootstrap round when `bootstrap` is set, or simply skipped when `externalBootstrap` is
  // set. So the exclusive round-number upper bound is always commits.size + 1 -- only the loop's
  // starting point (`startRound`) differs.
  def totalRounds: Int = commits.size + 1
  def startRound: Int = if (bootstrap.isDefined) 0 else 1
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
    // STRICT_DUPLICATE_DETECTION: a commit object with the same partition date twice is legal
    // JSON text, but Jackson's tree keeps only the last occurrence — silently dropping the
    // earlier counts. Fail loudly instead: duplicate dates almost certainly mean a bug in the
    // script that produced the spec.
    val mapper = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    val root =
      try {
        mapper.readTree(json)
      } catch {
        case e: JsonProcessingException =>
          throw new IllegalArgumentException(s"Workload spec is not valid JSON: ${e.getMessage}", e)
      }
    require(root != null && root.isObject, "Workload spec must be a JSON object")
    checkAllowedFields(
      root,
      Set("bootstrap", "commits", "nullifiedPartitions", "externalBootstrap"),
      "workload spec")

    val bootstrap = parseBootstrap(root)
    val externalBootstrap = parseExternalBootstrap(root)
    require(
      bootstrap.isDefined != externalBootstrap.isDefined,
      "Workload spec must have exactly one of 'bootstrap' or 'externalBootstrap'")

    val spec = FineGrainedWorkloadSpec(
      bootstrap,
      parseCommits(root),
      parseNullifiedPartitions(root),
      externalBootstrap)
    validateUpdateTargets(spec)
    spec
  }

  private def parseBootstrap(root: JsonNode): Option[BootstrapSpec] = {
    val node = root.get("bootstrap")
    if (node == null || node.isNull) {
      None
    } else {
      require(node.isObject, "'bootstrap' must be a JSON object")
      checkAllowedFields(
        node,
        Set("startDate", "endDate", "totalRecords", "suffixKeyWithPartitionPath"),
        "'bootstrap'")

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
      val suffixKeyWithPartitionPath = {
        val value = node.get("suffixKeyWithPartitionPath")
        if (value == null || value.isNull) false
        else {
          require(value.isBoolean, "'bootstrap.suffixKeyWithPartitionPath' must be a boolean")
          value.asBoolean()
        }
      }
      Some(BootstrapSpec(startDate, endDate, totalRecords, suffixKeyWithPartitionPath))
    }
  }

  private def parseExternalBootstrap(root: JsonNode): Option[ExternalBootstrapSpec] = {
    val node = root.get("externalBootstrap")
    if (node == null || node.isNull) {
      None
    } else {
      require(node.isObject, "'externalBootstrap' must be a JSON object")
      checkAllowedFields(
        node,
        Set(
          "tablePath",
          "payloadPoolMultiplier",
          "recordKeyField",
          "partitionPathField",
          "suffixKeyWithPartitionPath"),
        "'externalBootstrap'")

      val tablePath = requiredText(node, "tablePath", "externalBootstrap")
      val payloadPoolMultiplier = {
        val value = node.get("payloadPoolMultiplier")
        if (value == null || value.isNull) 2.0
        else {
          require(value.isNumber, "'externalBootstrap.payloadPoolMultiplier' must be a number")
          value.asDouble()
        }
      }
      require(
        payloadPoolMultiplier >= 1.0,
        s"externalBootstrap.payloadPoolMultiplier must be >= 1.0, got $payloadPoolMultiplier")
      val recordKeyField = {
        val value = node.get("recordKeyField")
        if (value == null || value.isNull) "_hoodie_record_key" else value.asText()
      }
      val partitionPathField = {
        val value = node.get("partitionPathField")
        if (value == null || value.isNull) "_hoodie_partition_path" else value.asText()
      }
      val suffixKeyWithPartitionPath = {
        val value = node.get("suffixKeyWithPartitionPath")
        if (value == null || value.isNull) false
        else {
          require(
            value.isBoolean,
            "'externalBootstrap.suffixKeyWithPartitionPath' must be a boolean")
          value.asBoolean()
        }
      }
      Some(
        ExternalBootstrapSpec(
          tablePath,
          payloadPoolMultiplier,
          recordKeyField,
          partitionPathField,
          suffixKeyWithPartitionPath))
    }
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

  private def parseNullifiedPartitions(root: JsonNode): Set[String] = {
    val node = root.get("nullifiedPartitions")
    if (node == null || node.isNull) {
      Set.empty
    } else {
      require(node.isArray, "'nullifiedPartitions' must be a JSON array of 'yyyy-MM-dd' dates")
      node
        .elements()
        .asScala
        .map { elem =>
          require(elem.isTextual, "'nullifiedPartitions' entries must be strings")
          parseDate(elem.asText(), "nullifiedPartitions entry").toString
        }
        .toSet
    }
  }

  /**
   * Updates can only target partitions that already hold data when the commit runs: a date
   * within the bootstrap range, or a date some *earlier* commit inserted into. A commit that
   * both opens a new partition and updates it is rejected — its own inserts are not visible
   * to its updates (updates are sampled from previously generated rounds only).
   *
   * Skipped entirely when `externalBootstrap` is set: any partition referenced by a commit's
   * `updates` is assumed to already exist in the external table, since lake-loader never
   * generated it and has no record of its true population.
   */
  private def validateUpdateTargets(spec: FineGrainedWorkloadSpec): Unit = {
    if (spec.externalBootstrap.isDefined) return
    val bootstrap = spec.bootstrap.get
    val insertedDates = mutable.Set[LocalDate]()
    spec.commits.zipWithIndex.foreach { case (commit, idx) =>
      val commitNumber = idx + 1
      commit.partitionOps.foreach { case (dateStr, ops) =>
        if (ops.updates > 0) {
          val date = LocalDate.parse(dateStr)
          val inBootstrapRange =
            !date.isBefore(bootstrap.startDate) && !date.isAfter(bootstrap.endDate)
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
