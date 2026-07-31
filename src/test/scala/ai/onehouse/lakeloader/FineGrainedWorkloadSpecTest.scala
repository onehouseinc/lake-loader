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

import ai.onehouse.lakeloader.configs.{ExternalBootstrapSpec, FineGrainedWorkloadSpec, PartitionOps}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class FineGrainedWorkloadSpecTest extends AnyFunSuite {

  private def parse(json: String): FineGrainedWorkloadSpec =
    FineGrainedWorkloadSpec.fromJsonString(json)

  private def expectInvalid(json: String, messageFragment: String): Unit = {
    val ex = intercept[IllegalArgumentException] { parse(json) }
    assert(
      ex.getMessage.contains(messageFragment),
      s"expected message containing '$messageFragment', got: '${ex.getMessage}'")
  }

  test("valid spec parses with bootstrap, commits, and derived rounds") {
    val spec = parse("""
      |{
      |  "bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 1000},
      |  "commits": [
      |    {"2026-01-02": {"inserts": 10, "updates": 5}, "2026-01-01": {"inserts": 20}},
      |    {"2026-01-06": {"inserts": 7}}
      |  ]
      |}""".stripMargin)

    assert(spec.bootstrap.get.startDate == LocalDate.parse("2026-01-01"))
    assert(spec.bootstrap.get.endDate == LocalDate.parse("2026-01-05"))
    assert(spec.bootstrap.get.totalRecords == 1000L)
    assert(spec.bootstrap.get.numPartitions == 5)
    assert(
      spec.bootstrap.get.partitionValues ==
        List("2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"))
    assert(spec.totalRounds == 3)
    assert(spec.startRound == 0)
    // partition entries are sorted ascending by date within a commit
    assert(
      spec.commits.head.partitionOps == List(
        ("2026-01-01", PartitionOps(20, 0)),
        ("2026-01-02", PartitionOps(10, 5))))
    assert(spec.commits(1).partitionOps == List(("2026-01-06", PartitionOps(7, 0))))
  }

  test("missing inserts/updates fields default to 0") {
    val spec = parse("""
      |{
      |  "bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01", "totalRecords": 10},
      |  "commits": [{"2026-01-01": {"updates": 3}}]
      |}""".stripMargin)
    assert(spec.commits.head.partitionOps == List(("2026-01-01", PartitionOps(0, 3))))
  }

  test("commits may be omitted entirely (bootstrap-only spec)") {
    val spec = parse(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-03", "totalRecords": 10}}""")
    assert(spec.commits.isEmpty)
    assert(spec.totalRounds == 1)
  }

  test("malformed JSON is rejected") {
    expectInvalid("{not json", "not valid JSON")
  }

  test("missing bootstrap and externalBootstrap is rejected") {
    expectInvalid(
      """{"commits": []}""",
      "must have exactly one of 'bootstrap' or 'externalBootstrap'")
  }

  test("specifying both bootstrap and externalBootstrap is rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01", "totalRecords": 10},
        | "externalBootstrap": {"tablePath": "hdfs://x/table"},
        | "commits": [{"2026-01-01": {"updates": 5}}]}""".stripMargin,
      "must have exactly one of 'bootstrap' or 'externalBootstrap'")
  }

  test("bad date format is rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026/01/01", "endDate": "2026-01-05", "totalRecords": 10}}""",
      "not a valid yyyy-MM-dd date")
  }

  test("startDate after endDate is rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-05", "endDate": "2026-01-01", "totalRecords": 10}}""",
      "must not be after")
  }

  test("non-positive totalRecords is rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 0}}""",
      "totalRecords must be > 0")
  }

  test("commits must be an array") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01", "totalRecords": 10},
        | "commits": {"2026-01-01": {"inserts": 1}}}""".stripMargin,
      "'commits' must be a JSON array")
  }

  test("empty commit object is rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01", "totalRecords": 10},
        | "commits": [{}]}""".stripMargin,
      "Commit #1 must be a non-empty JSON object")
  }

  test("negative counts are rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01", "totalRecords": 10},
        | "commits": [{"2026-01-01": {"inserts": -1, "updates": 5}}]}""".stripMargin,
      "must be >= 0")
  }

  test("all-zero partition entry is rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01", "totalRecords": 10},
        | "commits": [{"2026-01-01": {"inserts": 0, "updates": 0}}]}""".stripMargin,
      "at least one of inserts/updates must be > 0")
  }

  test("unknown field in a partition entry is rejected (typo guard)") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01", "totalRecords": 10},
        | "commits": [{"2026-01-01": {"insert": 5}}]}""".stripMargin,
      "Unknown field 'insert'")
  }

  test("unknown field in bootstrap is rejected (typo guard)") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-01",
        |               "totalRecords": 10, "records": 5}}""".stripMargin,
      "Unknown field 'records'")
  }

  // JSON objects with repeated keys are legal text but collapse to the last occurrence in a
  // tree parse — earlier counts would be silently dropped. STRICT_DUPLICATE_DETECTION makes
  // this fail loudly instead.
  test("duplicate partition dates within a commit are rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 10},
        | "commits": [{"2026-01-01": {"inserts": 100}, "2026-01-01": {"inserts": 900}}]}""".stripMargin,
      "Duplicate field")
  }

  test("duplicate top-level fields are rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 10},
        | "bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 99}}""".stripMargin,
      "Duplicate field")
  }

  test("updates to a partition with no prior data are rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 10},
        | "commits": [{"2026-02-01": {"updates": 5}}]}""".stripMargin,
      "has no data")
  }

  test("updates to a partition first inserted in the same commit are rejected") {
    expectInvalid(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 10},
        | "commits": [{"2026-02-01": {"inserts": 5, "updates": 5}}]}""".stripMargin,
      "has no data")
  }

  test("updates to a partition inserted by an earlier commit are allowed") {
    val spec = parse(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 10},
        | "commits": [
        |   {"2026-02-01": {"inserts": 5}},
        |   {"2026-02-01": {"updates": 3}}
        | ]}""".stripMargin)
    assert(spec.commits(1).partitionOps == List(("2026-02-01", PartitionOps(0, 3))))
  }

  test("inserts may open partitions outside the bootstrap date range") {
    val spec = parse(
      """{"bootstrap": {"startDate": "2026-01-01", "endDate": "2026-01-05", "totalRecords": 10},
        | "commits": [{"2027-06-15": {"inserts": 5}}]}""".stripMargin)
    assert(spec.commits.head.partitionOps == List(("2027-06-15", PartitionOps(5, 0))))
  }

  test("valid externalBootstrap spec parses with round numbering starting at 1") {
    val spec = parse("""
      |{
      |  "externalBootstrap": {"tablePath": "hdfs://x/table"},
      |  "commits": [
      |    {"2026-01-01": {"inserts": 10, "updates": 5}},
      |    {"2026-01-02": {"updates": 3}}
      |  ]
      |}""".stripMargin)
    assert(spec.bootstrap.isEmpty)
    assert(spec.externalBootstrap.contains(ExternalBootstrapSpec("hdfs://x/table")))
    assert(spec.startRound == 1)
    assert(spec.totalRounds == 3)
  }

  test("externalBootstrap defaults: payloadPoolMultiplier=2.0, hoodie meta field names") {
    val spec = parse("""
      |{
      |  "externalBootstrap": {"tablePath": "hdfs://x/table"},
      |  "commits": [{"2026-01-01": {"inserts": 5}}]
      |}""".stripMargin)
    val ext = spec.externalBootstrap.get
    assert(ext.payloadPoolMultiplier == 2.0)
    assert(ext.recordKeyField == "_hoodie_record_key")
    assert(ext.partitionPathField == "_hoodie_partition_path")
    assert(ext.suffixKeyWithPartitionPath == false)
  }

  test("externalBootstrap.suffixKeyWithPartitionPath can be enabled") {
    val spec = parse("""
      |{
      |  "externalBootstrap": {"tablePath": "hdfs://x/table", "suffixKeyWithPartitionPath": true},
      |  "commits": [{"2026-01-01": {"inserts": 5}}]
      |}""".stripMargin)
    assert(spec.externalBootstrap.get.suffixKeyWithPartitionPath == true)
  }

  test("externalBootstrap fields can be overridden") {
    val spec = parse("""
      |{
      |  "externalBootstrap": {
      |    "tablePath": "hdfs://x/table",
      |    "payloadPoolMultiplier": 3.5,
      |    "recordKeyField": "my_key",
      |    "partitionPathField": "my_partition"
      |  },
      |  "commits": [{"2026-01-01": {"inserts": 5}}]
      |}""".stripMargin)
    val ext = spec.externalBootstrap.get
    assert(ext.payloadPoolMultiplier == 3.5)
    assert(ext.recordKeyField == "my_key")
    assert(ext.partitionPathField == "my_partition")
  }

  test("externalBootstrap.payloadPoolMultiplier below 1.0 is rejected") {
    expectInvalid(
      """{"externalBootstrap": {"tablePath": "hdfs://x/table", "payloadPoolMultiplier": 0.5},
        | "commits": [{"2026-01-01": {"inserts": 5}}]}""".stripMargin,
      "payloadPoolMultiplier must be >= 1.0")
  }

  test("unknown field in externalBootstrap is rejected (typo guard)") {
    expectInvalid(
      """{"externalBootstrap": {"tablePath": "hdfs://x/table", "tablepath": "typo"},
        | "commits": [{"2026-01-01": {"inserts": 5}}]}""".stripMargin,
      "Unknown field 'tablepath'")
  }

  test("externalBootstrap skips the bootstrap-population check for updates") {
    // Would fail validateUpdateTargets under a normal bootstrap spec (no bootstrap range, no
    // earlier insert), but externalBootstrap assumes the partition already exists in the
    // external table, so this must parse without error.
    val spec = parse("""
      |{
      |  "externalBootstrap": {"tablePath": "hdfs://x/table"},
      |  "commits": [{"2026-01-01": {"updates": 5}}]
      |}""".stripMargin)
    assert(spec.commits.head.partitionOps == List(("2026-01-01", PartitionOps(0, 5))))
  }
}
