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

import ai.onehouse.lakeloader.ChangeDataGenerator.buildPartitionDistributionMatrix
import ai.onehouse.lakeloader.configs.PartitionDistributionSpec
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser

/**
 * Standalone runner that exercises [[ChangeDataGeneratorParser.parsePartitionDistribution]] and
 * [[ChangeDataGenerator.buildPartitionDistributionMatrix]] across the cases described in the
 * `--partition-distribution` help text. The repo has no scalatest/junit wired up, so this is a
 * `main`-style harness — run via `mvn scala:run` or `java` on the test classpath.
 *
 * Exits non-zero on any failure so it can be wired into a script later.
 */
object PartitionDistributionMatrixVerifier {

  private var failures = 0
  private val eps = 1e-9

  def main(args: Array[String]): Unit = {
    // 1. Flag omitted → no matrix (preserves the pre-change uniform-default behavior).
    expectMatrix(
      label = "flag omitted yields None (preserves default)",
      specOpt = None,
      totalPartitions = 365,
      numRounds = 10,
      expected = None)

    // 2. Single segment applies the same skew to every round.
    val sameSkewRow = List(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1) ++ List.fill(355)(0.0)
    expectMatrix(
      label = "single segment: same skew on every round",
      specOpt = Some(parse("0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1")),
      totalPartitions = 365,
      numRounds = 10,
      expected = Some(List.fill(10)(sameSkewRow)))

    // 3. The user's original Scala example: round 0 uniform across 365, rounds 1+ skewed.
    val uniform365 = List.fill(365)(1.0 / 365)
    val skewedRow = List.fill(10)(0.1) ++ List.fill(355)(0.0)
    expectMatrix(
      label = "';0.1,..0.1' → round 0 uniform, rounds 1+ skewed",
      specOpt = Some(parse(";0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1")),
      totalPartitions = 365,
      numRounds = 10,
      expected = Some(uniform365 :: List.fill(9)(skewedRow)))

    // 4. Round 0 skewed, rounds 1+ uniform — trailing empty segment.
    expectMatrix(
      label = "'0.1,..0.1;' → round 0 skewed, rounds 1+ uniform",
      specOpt = Some(parse("0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1;")),
      totalPartitions = 365,
      numRounds = 10,
      expected = Some(skewedRow :: List.fill(9)(uniform365)))

    // 5. Distinct first vs. subsequent: each segment zero-padded independently.
    expectMatrix(
      label = "'0.5,0.5;0.25,0.25,0.25,0.25' → distinct rows, each zero-padded",
      specOpt = Some(parse("0.5,0.5;0.25,0.25,0.25,0.25")),
      totalPartitions = 5,
      numRounds = 4,
      expected = Some(
        List(0.5, 0.5, 0.0, 0.0, 0.0) ::
          List.fill(3)(List(0.25, 0.25, 0.25, 0.25, 0.0))))

    // 6. numRounds == 1 falls back to firstRound only (the matrix-builder branch).
    expectMatrix(
      label = "numRounds=1 uses firstRound only",
      specOpt = Some(parse("0.5,0.5;0.25,0.25,0.25,0.25")),
      totalPartitions = 5,
      numRounds = 1,
      expected = Some(List(List(0.5, 0.5, 0.0, 0.0, 0.0))))

    // 7. numRounds == 0 → empty matrix (edge case; matches List.fill(0)(...)).
    expectMatrix(
      label = "numRounds=0 yields empty matrix",
      specOpt = Some(parse("0.5,0.5")),
      totalPartitions = 5,
      numRounds = 0,
      expected = Some(Nil))

    // 8. Sum-to-one assertion happens downstream in genPartitionsDistributionMatrix, not here.
    //    The builder itself should not reject 0.9-sum input; the downstream assert handles it.
    expectMatrix(
      label = "builder does not enforce sum=1.0 (delegated to genPartitionsDistributionMatrix)",
      specOpt = Some(parse("0.5,0.4")),
      totalPartitions = 5,
      numRounds = 2,
      expected = Some(List.fill(2)(List(0.5, 0.4, 0.0, 0.0, 0.0))))

    // 9. Negative: --total-partitions must be set.
    expectFailure(
      label = "missing --total-partitions",
      thunk = () =>
        buildPartitionDistributionMatrix(Some(parse("0.5,0.5")), totalPartitions = -1, numRounds = 5),
      expectedFragment = "--total-partitions must be set")

    // 10. Negative: segment longer than totalPartitions.
    expectFailure(
      label = "segment exceeds --total-partitions",
      thunk = () =>
        buildPartitionDistributionMatrix(
          Some(parse("0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1")),
          totalPartitions = 5,
          numRounds = 3),
      expectedFragment = "exceeds --total-partitions=5")

    // 11. Parser negative: more than one ';'.
    expectParseFailure(
      label = "parser rejects multiple ';' separators",
      raw = "0.5;0.3;0.2",
      expectedFragment = "at most one ';' separator")

    // 12. Parser edge: trailing/leading whitespace tolerated.
    val parsed = parse(" 0.5 , 0.5 ; 0.25 , 0.25 , 0.25 , 0.25 ")
    expectEquals(
      label = "parser trims whitespace",
      actual = parsed,
      expected = PartitionDistributionSpec(
        firstRound = Some(List(0.5, 0.5)),
        subsequentRounds = Some(List(0.25, 0.25, 0.25, 0.25))))

    // 13. Parser edge: single segment populates both fields with the same list (back-compat path).
    val singleSeg = parse("0.5,0.5")
    expectEquals(
      label = "single-segment spec mirrors firstRound into subsequentRounds",
      actual = singleSeg,
      expected = PartitionDistributionSpec(Some(List(0.5, 0.5)), Some(List(0.5, 0.5))))

    if (failures > 0) {
      System.err.println(s"\n$failures check(s) FAILED")
      sys.exit(1)
    } else {
      println("\nAll partition-distribution matrix checks PASSED")
    }
  }

  private def parse(raw: String): PartitionDistributionSpec =
    ChangeDataGeneratorParser.parsePartitionDistribution(raw)

  private def expectMatrix(
      label: String,
      specOpt: Option[PartitionDistributionSpec],
      totalPartitions: Int,
      numRounds: Int,
      expected: Option[List[List[Double]]]): Unit = {
    val actual = buildPartitionDistributionMatrix(specOpt, totalPartitions, numRounds)
    val ok = (actual, expected) match {
      case (None, None) => true
      case (Some(a), Some(e)) => matricesEqual(a, e)
      case _ => false
    }
    record(label, ok, s"expected=$expected actual=$actual")
  }

  private def expectEquals[A](label: String, actual: A, expected: A): Unit =
    record(label, actual == expected, s"expected=$expected actual=$actual")

  private def expectFailure(
      label: String,
      thunk: () => Any,
      expectedFragment: String): Unit = {
    val outcome =
      try {
        thunk()
        Left("did not throw")
      } catch {
        case e: Throwable => Right(e.getMessage)
      }
    outcome match {
      case Right(msg) if msg != null && msg.contains(expectedFragment) =>
        record(label, ok = true, "")
      case Right(msg) =>
        record(label, ok = false, s"thrown msg='$msg' did not contain '$expectedFragment'")
      case Left(reason) =>
        record(label, ok = false, reason)
    }
  }

  private def expectParseFailure(
      label: String,
      raw: String,
      expectedFragment: String): Unit =
    expectFailure(label, () => parse(raw), expectedFragment)

  private def matricesEqual(a: List[List[Double]], b: List[List[Double]]): Boolean = {
    if (a.size != b.size) return false
    a.zip(b).forall { case (rowA, rowB) =>
      rowA.size == rowB.size && rowA.zip(rowB).forall { case (x, y) => math.abs(x - y) < eps }
    }
  }

  private def record(label: String, ok: Boolean, detail: String): Unit = {
    if (ok) println(s"  PASS  $label")
    else {
      failures += 1
      println(s"  FAIL  $label  ($detail)")
    }
  }
}
