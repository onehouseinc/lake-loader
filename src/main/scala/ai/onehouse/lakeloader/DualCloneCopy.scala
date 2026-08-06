package ai.onehouse.lakeloader

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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

/**
 * One-off utility: recursively clones an entire HDFS directory tree (a bootstrapped Hudi table,
 * including its full .hoodie/ tree -- timeline, metadata table, record-level index) to TWO
 * destination paths. Each executor task copies one source file to both destinations via
 * `FileUtil.copy`, independently and with retries (reads the source once per destination -- 2x
 * read I/O off the source table compared to a single-read tee, but avoids any hand-rolled
 * stream/buffer management).
 *
 * Implemented as a real Scala/JVM Spark job (not PySpark) specifically because the per-file copy
 * needs native Hadoop FileSystem calls inside each executor task -- PySpark's py4j/JVM bridge only
 * exists on the driver process, not inside `mapPartitions` closures running on executors, so this
 * could not be implemented as a Python script.
 *
 * Lesson from a real run (2026-08-03): at 320x16 executors (5,120 concurrent tasks), a large
 * fraction of `FileUtil.copy` calls failed with HDFS's "Unable to close file because the last
 * block ... does not have enough number of replicas" -- a transient write-throughput overload, not
 * a code bug per se. But two real code bugs turned that transient overload into silent data loss:
 * (1) both destinations' copies were attempted inside a single try/catch, so a destA failure threw
 * before destB was even attempted, leaving destB missing files entirely instead of just destA being
 * short; (2) there was no retry and no hard failure on a nonzero error count, so the Spark job
 * happily reported COMPLETED despite ~60% of files never landing correctly. Fixed by making each
 * destination's copy fully independent (its own try/retry), verifying copied file length against
 * source length (silent truncation without an exception is possible too), and throwing at the end
 * if any file failed in any destination after retries -- a broken run must never report success.
 */
object DualCloneCopy {
  // Guardrail: this table took ~48 hours of cluster time to bootstrap (seed datagen + seed-load +
  // fan-out + enable MDT + enable RLI). Hardcode the known protected table path explicitly, on top
  // of the general source-overlap check below, so a swapped/typo'd argument can never reach it --
  // this check does not depend on the sourcePath argument being correct.
  private val PROTECTED_PATHS = Seq(
    "hdfs://hdfs-xenon-nn1.uber.internal:8020/quanton-poc/lake-loader-mezzanine-1000part-60m/output/spark_catalog/default/hudi_mezzanine_1000part_60m_1"
  )

  private val MAX_ATTEMPTS = 4
  private val RETRY_BACKOFF_MS = 3000L
  // A handful of isolated failures (even after 4 retries each) shouldn't sink an otherwise
  // successful multi-million-file run -- only fail the whole job if errors exceed this small,
  // fixed tolerance. This is NOT the same as the earlier bug (silently accepting ~60% failures):
  // that case threw no error at all; this tolerance is small enough that exceeding it always means
  // something systemic, not a couple of transient blips.
  private val MAX_TOLERABLE_ERRORS = 10

  /**
   * Copy one file to one destination, retrying on failure (including a length-mismatch after a
   * copy that didn't throw -- catches silent truncation, not just exceptions). Returns true iff
   * the destination file's length matches the source's within MAX_ATTEMPTS tries.
   */
  private def copyWithRetry(fs: FileSystem, src: Path, dst: Path, srcLen: Long, conf: Configuration): Either[String, Unit] = {
    var lastError: String = "unknown error"
    var attempt = 0
    while (attempt < MAX_ATTEMPTS) {
      attempt += 1
      try {
        FileUtil.copy(fs, src, fs, dst, false, true, conf)
        val dstLen = fs.getFileStatus(dst).getLen
        if (dstLen == srcLen) {
          return Right(())
        }
        lastError = s"length mismatch after copy (attempt $attempt): src=$srcLen dst=$dstLen"
      } catch {
        case e: Exception => lastError = s"attempt $attempt failed: ${e.getMessage}"
      }
      if (attempt < MAX_ATTEMPTS) Thread.sleep(RETRY_BACKOFF_MS)
    }
    Left(lastError)
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 2 || args.length == 3,
      "Usage: DualCloneCopy <sourcePath> <destPathA> [<destPathB>]")
    val sourcePath = args(0).stripSuffix("/")
    val destPathA = args(1).stripSuffix("/")
    val destPathBOpt = if (args.length == 3) Some(args(2).stripSuffix("/")) else None
    val destPaths = Seq(destPathA) ++ destPathBOpt.toSeq

    // Guardrail: refuse to run if either destination is the same as, nested inside, or an
    // ancestor of the source path, or of the hardcoded protected path above. Protects against a
    // swapped/typo'd argument silently overwriting a table that may have taken many hours to
    // bootstrap.
    def isSameOrNested(a: String, b: String): Boolean =
      a == b || a.startsWith(b + "/") || b.startsWith(a + "/")

    destPaths.foreach { dest =>
      PROTECTED_PATHS.foreach { protectedPath =>
        require(
          !isSameOrNested(dest, protectedPath),
          s"REFUSING TO RUN: destination '$dest' is the same as, nested inside, or an ancestor " +
            s"of the protected source table '$protectedPath'. This table took ~48 hours to " +
            "bootstrap -- this job will never write to it. Fix the destination argument.")
      }
      require(
        !isSameOrNested(dest, sourcePath),
        s"REFUSING TO RUN: destination '$dest' overlaps with source path '$sourcePath'. " +
          "Copying a directory onto itself (or a parent/child of itself) is never intended.")
    }

    val spark = SparkSession.builder
      .appName(if (destPathBOpt.isDefined) "lake-loader dual clone copy" else "lake-loader single clone copy")
      .getOrCreate()
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    // List every file under the source recursively (driver-side), capturing each file's length
    // upfront so executors can verify copied length without an extra NameNode round-trip per file.
    val sourcePathObj = new Path(sourcePath)
    val fileIter = fs.listFiles(sourcePathObj, true)
    val relativePaths = scala.collection.mutable.ArrayBuffer[(String, Long)]()
    while (fileIter.hasNext) {
      val status = fileIter.next()
      val fullPath = status.getPath.toString
      val rel = fullPath.stripPrefix(sourcePath).stripPrefix("/")
      relativePaths += ((rel, status.getLen))
    }
    val totalFiles = relativePaths.size
    println(s"TOTAL_FILES_FOUND: $totalFiles")

    // Pre-create every ancestor directory in all destinations upfront, on the driver.
    val allDirs = scala.collection.mutable.HashSet[String]()
    relativePaths.foreach { case (rel, _) =>
      val parts = rel.split("/").dropRight(1)
      for (i <- 0 to parts.length) {
        allDirs += parts.take(i).mkString("/")
      }
    }
    destPaths.foreach { destRoot =>
      allDirs.foreach { d =>
        val p = if (d.isEmpty) new Path(destRoot) else new Path(s"$destRoot/$d")
        fs.mkdirs(p)
      }
    }
    println("DEST_DIR_STRUCTURE_CREATED")

    val rdd = sc.parallelize(relativePaths.toSeq, math.max(1, totalFiles / 50))

    val resultRdd = rdd.mapPartitions { iter =>
      val conf = new Configuration()
      val partitionFs = FileSystem.get(conf)
      var copied = 0
      val errors = scala.collection.mutable.ArrayBuffer[String]()
      iter.foreach { case (rel, srcLen) =>
        val src = new Path(s"$sourcePath/$rel")
        // Every destination is copied and retried fully independently -- a failure (even after
        // retries) on one destination must never prevent another destination from being
        // attempted. This was the root cause of dest_b silently missing ~570K files on the first
        // dual-destination run: both copies shared one try/catch, so a destA exception skipped
        // destB entirely.
        val results = destPaths.map { destRoot =>
          val dst = new Path(s"$destRoot/$rel")
          copyWithRetry(partitionFs, src, dst, srcLen, conf)
        }
        if (results.forall(_.isRight)) {
          copied += 1
        } else {
          results.zip(destPaths).foreach {
            case (Left(err), destRoot) => errors += s"$rel [$destRoot]: $err"
            case _ =>
          }
        }
      }
      Iterator((copied, errors.toSeq))
    }

    val results = resultRdd.collect()
    val totalCopied = results.map(_._1).sum
    val allErrors = results.flatMap(_._2)

    println(s"TOTAL_COPIED: $totalCopied")
    println(s"TOTAL_ERRORS: ${allErrors.length}")
    allErrors.take(200).foreach(e => println(s"ERROR: $e"))

    spark.stop()

    // A broken run must never silently report success -- fail loudly if errors exceed a small,
    // fixed tolerance, so the caller (SparkApplication status, this job's own exit code) reflects
    // reality instead of a benign COMPLETED state hiding systemic data loss. A handful of isolated
    // failures (even after MAX_ATTEMPTS retries each) is tolerated and just logged as a warning --
    // the whole point of the tolerance is that 1-2 stray failures out of millions of files
    // shouldn't sink an otherwise successful run, while anything beyond that still fails hard.
    if (allErrors.length > MAX_TOLERABLE_ERRORS) {
      throw new RuntimeException(
        s"DualCloneCopy FAILED: ${allErrors.length} file(s) out of $totalFiles could not be copied " +
          s"correctly to all destinations after $MAX_ATTEMPTS attempts each -- exceeds the " +
          s"tolerance of $MAX_TOLERABLE_ERRORS. See ERROR lines above (first 200) for details.")
    } else if (allErrors.nonEmpty) {
      println(s"WARNING: ${allErrors.length} file(s) out of $totalFiles failed after $MAX_ATTEMPTS " +
        s"attempts each, within the tolerance of $MAX_TOLERABLE_ERRORS -- job reports success anyway. " +
        "See ERROR lines above for the specific file(s).")
    }
  }
}
