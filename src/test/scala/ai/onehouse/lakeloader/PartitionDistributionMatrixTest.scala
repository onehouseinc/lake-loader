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
import org.scalatest.funsuite.AnyFunSuite

class PartitionDistributionMatrixTest extends AnyFunSuite {

  private val eps = 1e-9

  private def parse(raw: String): PartitionDistributionSpec =
    ChangeDataGeneratorParser.parsePartitionDistribution(raw)

  private def assertMatrixEquals(
      actual: Option[List[List[Double]]],
      expected: Option[List[List[Double]]]): Unit = {
    (actual, expected) match {
      case (None, None) => ()
      case (Some(a), Some(e)) =>
        assert(a.size == e.size, s"row counts differ: ${a.size} vs ${e.size}")
        a.zip(e).zipWithIndex.foreach { case ((rowA, rowE), idx) =>
          assert(
            rowA.size == rowE.size,
            s"row $idx widths differ: ${rowA.size} vs ${rowE.size}")
          rowA.zip(rowE).zipWithIndex.foreach { case ((x, y), col) =>
            assert(math.abs(x - y) < eps, s"row $idx col $col: $x vs $y")
          }
        }
      case _ => fail(s"shape mismatch: expected=$expected actual=$actual")
    }
  }

  test("flag omitted yields None (preserves default uniform-matrix behavior)") {
    assert(buildPartitionDistributionMatrix(None, 365, 10).isEmpty)
  }

  test("single segment applies the same skew to every round") {
    val expectedRow =
      List(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1) ++ List.fill(355)(0.0)
    val matrix = buildPartitionDistributionMatrix(
      Some(parse("0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1")),
      totalPartitions = 365,
      numRounds = 10)
    assertMatrixEquals(matrix, Some(List.fill(10)(expectedRow)))
  }

  test("';0.1,..' makes round 0 uniform and rounds 1+ skewed") {
    val uniform365 = List.fill(365)(1.0 / 365)
    val skewedRow = List.fill(10)(0.1) ++ List.fill(355)(0.0)
    val matrix = buildPartitionDistributionMatrix(
      Some(parse(";0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1")),
      totalPartitions = 365,
      numRounds = 10)
    assertMatrixEquals(matrix, Some(uniform365 :: List.fill(9)(skewedRow)))
  }

  test("'0.1,..;' makes round 0 skewed and rounds 1+ uniform") {
    val uniform365 = List.fill(365)(1.0 / 365)
    val skewedRow = List.fill(10)(0.1) ++ List.fill(355)(0.0)
    val matrix = buildPartitionDistributionMatrix(
      Some(parse("0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1;")),
      totalPartitions = 365,
      numRounds = 10)
    assertMatrixEquals(matrix, Some(skewedRow :: List.fill(9)(uniform365)))
  }

  test("distinct first vs subsequent segments are zero-padded independently") {
    val matrix = buildPartitionDistributionMatrix(
      Some(parse("0.5,0.5;0.25,0.25,0.25,0.25")),
      totalPartitions = 5,
      numRounds = 4)
    val expected =
      List(0.5, 0.5, 0.0, 0.0, 0.0) ::
        List.fill(3)(List(0.25, 0.25, 0.25, 0.25, 0.0))
    assertMatrixEquals(matrix, Some(expected))
  }

  test("numRounds=1 produces a single row from firstRound") {
    val matrix = buildPartitionDistributionMatrix(
      Some(parse("0.5,0.5;0.25,0.25,0.25,0.25")),
      totalPartitions = 5,
      numRounds = 1)
    assertMatrixEquals(matrix, Some(List(List(0.5, 0.5, 0.0, 0.0, 0.0))))
  }

  test("numRounds=0 produces an empty matrix") {
    val matrix = buildPartitionDistributionMatrix(
      Some(parse("0.5,0.5")),
      totalPartitions = 5,
      numRounds = 0)
    assertMatrixEquals(matrix, Some(Nil))
  }

  test("builder does not enforce sum=1.0 (delegated to genPartitionsDistributionMatrix)") {
    val matrix = buildPartitionDistributionMatrix(
      Some(parse("0.5,0.4")),
      totalPartitions = 5,
      numRounds = 2)
    assertMatrixEquals(matrix, Some(List.fill(2)(List(0.5, 0.4, 0.0, 0.0, 0.0))))
  }

  test("missing --total-partitions is rejected") {
    val ex = intercept[IllegalArgumentException] {
      buildPartitionDistributionMatrix(Some(parse("0.5,0.5")), totalPartitions = -1, numRounds = 5)
    }
    assert(ex.getMessage.contains("--total-partitions must be set"))
  }

  test("segment longer than --total-partitions is rejected") {
    val ex = intercept[IllegalArgumentException] {
      buildPartitionDistributionMatrix(
        Some(parse("0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1")),
        totalPartitions = 5,
        numRounds = 3)
    }
    assert(ex.getMessage.contains("exceeds --total-partitions=5"))
  }

  test("parser rejects more than one ';' separator") {
    val ex = intercept[IllegalArgumentException] {
      parse("0.5;0.3;0.2")
    }
    assert(ex.getMessage.contains("at most one ';' separator"))
  }

  test("parser tolerates leading/trailing whitespace inside segments") {
    val parsed = parse(" 0.5 , 0.5 ; 0.25 , 0.25 , 0.25 , 0.25 ")
    assert(parsed == PartitionDistributionSpec(
      firstRound = Some(List(0.5, 0.5)),
      subsequentRounds = Some(List(0.25, 0.25, 0.25, 0.25))))
  }

  test("single-segment spec mirrors firstRound into subsequentRounds") {
    val parsed = parse("0.5,0.5")
    assert(parsed == PartitionDistributionSpec(Some(List(0.5, 0.5)), Some(List(0.5, 0.5))))
  }
}
