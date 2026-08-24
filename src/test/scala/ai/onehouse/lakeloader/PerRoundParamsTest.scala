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

import ai.onehouse.lakeloader.ChangeDataGenerator.{padOrTruncate, valueForRound}
import ai.onehouse.lakeloader.configs.{DatagenConfig, UpdatePatterns}
import ai.onehouse.lakeloader.parser.ChangeDataGeneratorParser
import org.scalatest.funsuite.AnyFunSuite

class PerRoundParamsTest extends AnyFunSuite {

  ///////////////////////
  // valueForRound
  ///////////////////////

  test("valueForRound broadcasts a scalar (single-entry list) to every round") {
    val xs = List(0.5)
    assert(valueForRound(xs, 0) == 0.5)
    assert(valueForRound(xs, 42) == 0.5)
  }

  test("valueForRound picks exact per-round value when list has enough entries") {
    val xs = List(0.1, 0.2, 0.3, 0.4, 0.5)
    assert(valueForRound(xs, 0) == 0.1)
    assert(valueForRound(xs, 2) == 0.3)
    assert(valueForRound(xs, 4) == 0.5)
  }

  test("valueForRound fills with last value when round exceeds list length") {
    val xs = List(0.1, 0.2, 0.3)
    assert(valueForRound(xs, 3) == 0.3)
    assert(valueForRound(xs, 99) == 0.3)
  }

  test("valueForRound rejects empty list") {
    intercept[IllegalArgumentException] {
      valueForRound(List.empty[Double], 0)
    }
  }

  ///////////////////////
  // padOrTruncate
  ///////////////////////

  test("padOrTruncate pads a shorter list by repeating the last value") {
    val out = padOrTruncate(List(1, 2, 3), 5)
    assert(out == List(1, 2, 3, 3, 3))
  }

  test("padOrTruncate truncates a longer list to exactly n entries") {
    val out = padOrTruncate(List(1, 2, 3, 4, 5, 6, 7), 3)
    assert(out == List(1, 2, 3))
  }

  test("padOrTruncate leaves a matching-length list unchanged") {
    val xs = List(10, 20, 30)
    assert(padOrTruncate(xs, 3) == xs)
  }

  test("padOrTruncate broadcasts a scalar to all n rounds") {
    assert(padOrTruncate(List(7), 4) == List(7, 7, 7, 7))
  }

  test("padOrTruncate rejects empty input") {
    intercept[IllegalArgumentException] {
      padOrTruncate(List.empty[Int], 3)
    }
  }

  ///////////////////////
  // Parser: single value broadcasts, list is per-round
  ///////////////////////

  private def parse(args: Array[String]): DatagenConfig =
    ChangeDataGeneratorParser.parser.parse(args, DatagenConfig()).getOrElse(
      fail(s"parser rejected args: ${args.mkString(" ")}"))

  test("--update-ratio accepts a single value") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-ratio", "0.42"))
    assert(cfg.updateRatios == List(0.42))
  }

  test("--update-ratio accepts a per-round comma-separated list") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-ratio", "0.1,0.5,0.8,0.3"))
    assert(cfg.updateRatios == List(0.1, 0.5, 0.8, 0.3))
  }

  test("--num-partitions-to-update accepts a single value") {
    val cfg = parse(Array("--path", "/tmp/out", "--num-partitions-to-update", "20"))
    assert(cfg.numPartitionsToUpdate == List(20))
  }

  test("--num-partitions-to-update accepts a per-round list") {
    val cfg = parse(Array("--path", "/tmp/out", "--num-partitions-to-update", "5,10,15,20"))
    assert(cfg.numPartitionsToUpdate == List(5, 10, 15, 20))
  }

  test("--zipfian-shape accepts a single value") {
    val cfg = parse(Array("--path", "/tmp/out", "--zipfian-shape", "1.5"))
    assert(cfg.zipfianShapes == List(1.5))
  }

  test("--zipfian-shape accepts a per-round list") {
    val cfg = parse(Array("--path", "/tmp/out", "--zipfian-shape", "1.0,1.5,2.0,2.93"))
    assert(cfg.zipfianShapes == List(1.0, 1.5, 2.0, 2.93))
  }

  test("--update-pattern accepts a single value") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-pattern", "Zipf"))
    assert(cfg.updatePatterns == List(UpdatePatterns.Zipf))
  }

  test("--update-pattern accepts a per-round list") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-pattern", "Uniform,Zipf,Uniform,Zipf"))
    assert(cfg.updatePatterns == List(
      UpdatePatterns.Uniform, UpdatePatterns.Zipf, UpdatePatterns.Uniform, UpdatePatterns.Zipf))
  }

  test("--update-pattern rejects an unknown value") {
    // scopt catches thrown IllegalArgumentException from opt actions and turns the parse
    // into a None (with an error message printed). Assert that shape.
    val result = ChangeDataGeneratorParser.parser.parse(
      Array("--path", "/tmp/out", "--update-pattern", "Uniform,Bogus"),
      DatagenConfig())
    assert(result.isEmpty, s"expected parser to reject unknown pattern, got: $result")
  }

  test("--update-ratio tolerates whitespace around commas") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-ratio", "0.1, 0.5 ,0.8"))
    assert(cfg.updateRatios == List(0.1, 0.5, 0.8))
  }

  ///////////////////////
  // End-to-end: padOrTruncate + valueForRound compose correctly
  ///////////////////////

  test("scalar --update-ratio broadcasts through padOrTruncate to every round") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-ratio", "0.3"))
    val padded = padOrTruncate(cfg.updateRatios, 5)
    assert(padded == List(0.3, 0.3, 0.3, 0.3, 0.3))
    (0 until 5).foreach(r => assert(valueForRound(padded, r) == 0.3))
  }

  test("short list --update-ratio right-pads with last value") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-ratio", "0.1,0.5"))
    val padded = padOrTruncate(cfg.updateRatios, 5)
    // 0.1, 0.5, then 0.5 (last) repeated
    assert(padded == List(0.1, 0.5, 0.5, 0.5, 0.5))
  }

  test("longer list --update-ratio truncates to numberOfRounds") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-ratio", "0.1,0.2,0.3,0.4,0.5,0.6,0.7"))
    val padded = padOrTruncate(cfg.updateRatios, 4)
    assert(padded == List(0.1, 0.2, 0.3, 0.4))
  }

  test("per-round --update-pattern round-trips through padOrTruncate and valueForRound") {
    val cfg = parse(Array("--path", "/tmp/out", "--update-pattern", "Uniform,Zipf"))
    val padded = padOrTruncate(cfg.updatePatterns, 5)
    assert(padded == List(
      UpdatePatterns.Uniform, UpdatePatterns.Zipf, UpdatePatterns.Zipf,
      UpdatePatterns.Zipf, UpdatePatterns.Zipf))
  }

  test("padOrTruncate(list, 0) returns empty regardless of input length") {
    assert(padOrTruncate(List(1, 2, 3), 0) == Nil)
    assert(padOrTruncate(List(42), 0) == Nil)
  }

  test("--num-partitions-to-update accepts -1 as unbounded sentinel") {
    // Any list mixing -1 with valid partition counts should parse.
    val cfg = parse(Array("--path", "/tmp/out", "--num-partitions-to-update", "-1,10,-1"))
    assert(cfg.numPartitionsToUpdate == List(-1, 10, -1))
  }

  test("deprecated scalar-overload of generateWorkload is still resolvable via reflection") {
    // Cheap check that both overloads exist on the class: the current
    // list-based method and the @deprecated scalar-parameter one. Uses
    // reflection to avoid actually invoking a Spark job.
    val cls = classOf[ChangeDataGenerator]
    val methods = cls.getMethods.filter(_.getName == "generateWorkload")
    assert(methods.length >= 2,
      s"expected at least 2 overloads of generateWorkload, found ${methods.length}: " +
        methods.map(m => m.getParameterTypes.map(_.getSimpleName).mkString(",")).mkString("; "))
    // Scalar overload: primitive Double + primitive Int (updateRatio, numPartitionsToUpdate).
    val scalarSig = methods.find { m =>
      val paramTypes = m.getParameterTypes
      paramTypes.exists(_ == java.lang.Double.TYPE) &&
        paramTypes.exists(_ == java.lang.Integer.TYPE)
    }
    assert(scalarSig.isDefined,
      s"no generateWorkload overload with (double, int) primitives — deprecated scalar overload missing")
    // New overload: uses scala.collection.immutable.List for the four per-round params.
    val listSig = methods.find { m =>
      m.getParameterTypes.count(_ == classOf[scala.collection.immutable.List[_]]) >= 4
    }
    assert(listSig.isDefined,
      "no generateWorkload overload with 4+ List params — new list-based signature missing")
  }
}
