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
import org.apache.hadoop.fs.{FileChecksum, FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

/**
 * Reconciles a partially-copied destination against its source: for every source file, compares
 * against the destination by (a) existence, (b) length, (c) HDFS composite checksum
 * (`FileSystem.getFileChecksum`) -- if the destination file is missing OR its checksum doesn't
 * match the source's, it's (re)copied with the same retry/length-verification logic as
 * DualCloneCopy. Built to finish off a run like DualCloneCopy's that legitimately failed
 * (hard `throw`, not silently reported success) partway through due to HDFS write-throughput
 * overload -- rather than re-copying all ~1M files from scratch, only the actually-broken subset
 * needs to be redone.
 *
 * Two-phase, both distributed across executors (comparison and copy are separate Spark jobs so the
 * (usually much smaller) copy-candidate list is known before any copy work starts, and so progress
 * for each phase is independently visible):
 *   1. Compare: for every source file, look up its destination counterpart via a broadcast map of
 *      the destination's already-listed (relativePath -> length) pairs (built driver-side, same
 *      pattern as DualCloneCopy's source listing). If the destination is missing the file or its
 *      length differs, it's flagged as needing copy without ever computing a checksum (an unrelated
 *      truncation/missing case is far cheaper to detect this way). Only for files whose length
 *      matches is `getFileChecksum` computed for both sides and compared -- length-matching but
 *      checksum-mismatched files are the "invisible" case a length-only check like the original
 *      DualCloneCopy verification would miss entirely.
 *   2. Copy: only the flagged subset is (re)copied, via the same independent-retry
 *      (`copyWithRetry`, 4 attempts, 3s backoff, post-copy length check) and error-tolerance
 *      (`MAX_TOLERABLE_ERRORS`) pattern as DualCloneCopy.
 */
object ReconcileCopy {
  // Guardrail: same hardcoded protected source table as DualCloneCopy -- this reconciliation job
  // must never be pointed at it as a destination either.
  private val PROTECTED_PATHS = Seq(
    "hdfs://hdfs-xenon-nn1.uber.internal:8020/quanton-poc/lake-loader-mezzanine-1000part-60m/output/spark_catalog/default/hudi_mezzanine_1000part_60m_1"
  )

  private val MAX_ATTEMPTS = 4
  private val RETRY_BACKOFF_MS = 3000L
  private val MAX_TOLERABLE_ERRORS = 10

  private def checksumsMatch(fs: FileSystem, src: Path, dst: Path): Boolean = {
    try {
      val srcSum: FileChecksum = fs.getFileChecksum(src)
      val dstSum: FileChecksum = fs.getFileChecksum(dst)
      if (srcSum == null || dstSum == null) false else srcSum.equals(dstSum)
    } catch {
      // Checksum comparison can throw if source/destination were written with incompatible
      // checksum params (e.g. different bytesPerCRC) -- treat as "can't verify, so don't trust it"
      // rather than crashing the compare stage over one file.
      case _: Exception => false
    }
  }

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
    require(args.length == 2, "Usage: ReconcileCopy <sourcePath> <destPath>")
    val sourcePath = args(0).stripSuffix("/")
    val destPath = args(1).stripSuffix("/")

    def isSameOrNested(a: String, b: String): Boolean =
      a == b || a.startsWith(b + "/") || b.startsWith(a + "/")

    PROTECTED_PATHS.foreach { protectedPath =>
      require(
        !isSameOrNested(destPath, protectedPath),
        s"REFUSING TO RUN: destination '$destPath' is the same as, nested inside, or an ancestor " +
          s"of the protected source table '$protectedPath'. This table took ~48 hours to " +
          "bootstrap -- this job will never write to it. Fix the destination argument.")
    }
    require(
      !isSameOrNested(destPath, sourcePath),
      s"REFUSING TO RUN: destination '$destPath' overlaps with source path '$sourcePath'. " +
        "Copying a directory onto itself (or a parent/child of itself) is never intended.")

    val spark = SparkSession.builder.appName("lake-loader reconcile copy").getOrCreate()
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    val sourcePathObj = new Path(sourcePath)
    val srcIter = fs.listFiles(sourcePathObj, true)
    val sourceFiles = scala.collection.mutable.ArrayBuffer[(String, Long)]()
    while (srcIter.hasNext) {
      val status = srcIter.next()
      val rel = status.getPath.toString.stripPrefix(sourcePath).stripPrefix("/")
      sourceFiles += ((rel, status.getLen))
    }
    println(s"TOTAL_SOURCE_FILES: ${sourceFiles.size}")

    val destPathObj = new Path(destPath)
    val destMapBuilder = scala.collection.mutable.HashMap[String, Long]()
    if (fs.exists(destPathObj)) {
      val dstIter = fs.listFiles(destPathObj, true)
      while (dstIter.hasNext) {
        val status = dstIter.next()
        val rel = status.getPath.toString.stripPrefix(destPath).stripPrefix("/")
        destMapBuilder += ((rel, status.getLen))
      }
    }
    println(s"TOTAL_DEST_FILES_FOUND: ${destMapBuilder.size}")
    val destMapBroadcast = sc.broadcast(destMapBuilder.toMap)

    // Phase 1: compare. Length mismatch (including missing, treated as length -1) short-circuits
    // straight to "needs copy" without a checksum call; only length-matching files pay for
    // getFileChecksum on both sides.
    val compareRdd = sc.parallelize(sourceFiles.toSeq, math.max(1, sourceFiles.size / 50))
    val mismatchRdd = compareRdd.mapPartitions { iter =>
      val conf = new Configuration()
      val partitionFs = FileSystem.get(conf)
      val destMap = destMapBroadcast.value
      iter.flatMap { case (rel, srcLen) =>
        destMap.get(rel) match {
          case None => Some((rel, srcLen))
          case Some(dstLen) if dstLen != srcLen => Some((rel, srcLen))
          case Some(_) =>
            val src = new Path(s"$sourcePath/$rel")
            val dst = new Path(s"$destPath/$rel")
            if (checksumsMatch(partitionFs, src, dst)) None else Some((rel, srcLen))
        }
      }
    }
    val mismatched = mismatchRdd.collect()
    println(s"TOTAL_NEEDING_COPY: ${mismatched.length}")

    if (mismatched.isEmpty) {
      println("RECONCILE_RESULT: already fully in sync, nothing to copy")
      spark.stop()
      return
    }

    // Pre-create ancestor directories for the files being (re)copied -- mostly already exist, but
    // this is idempotent and cheap, and covers the case where a whole leaf directory was skipped
    // by the prior run.
    val allDirs = scala.collection.mutable.HashSet[String]()
    mismatched.foreach { case (rel, _) =>
      val parts = rel.split("/").dropRight(1)
      for (i <- 0 to parts.length) {
        allDirs += parts.take(i).mkString("/")
      }
    }
    allDirs.foreach { d =>
      val p = if (d.isEmpty) destPathObj else new Path(s"$destPath/$d")
      fs.mkdirs(p)
    }

    // Phase 2: copy only the flagged subset -- one task per file (unlike phase 1's ~50-files/task
    // batching), since this set is expected to be a much smaller fraction of the full table (~10%
    // per the observed failure rate) and each file's copy is the actual expensive/retryable
    // network operation, so maximizing per-file parallelism matters more here than batching
    // overhead does.
    val copyRdd = sc.parallelize(mismatched.toSeq, math.max(1, mismatched.length))
    val resultRdd = copyRdd.mapPartitions { iter =>
      val conf = new Configuration()
      val partitionFs = FileSystem.get(conf)
      var copied = 0
      val errors = scala.collection.mutable.ArrayBuffer[String]()
      iter.foreach { case (rel, srcLen) =>
        val src = new Path(s"$sourcePath/$rel")
        val dst = new Path(s"$destPath/$rel")
        copyWithRetry(partitionFs, src, dst, srcLen, conf) match {
          case Right(_) => copied += 1
          case Left(err) => errors += s"$rel: $err"
        }
      }
      Iterator((copied, errors.toSeq))
    }

    val results = resultRdd.collect()
    val totalCopied = results.map(_._1).sum
    val allErrors = results.flatMap(_._2)

    println(s"TOTAL_RECOPIED: $totalCopied")
    println(s"TOTAL_RECOPY_ERRORS: ${allErrors.length}")
    allErrors.take(200).foreach(e => println(s"ERROR: $e"))

    spark.stop()

    if (allErrors.length > MAX_TOLERABLE_ERRORS) {
      throw new RuntimeException(
        s"ReconcileCopy FAILED: ${allErrors.length} file(s) out of ${mismatched.length} flagged " +
          s"could not be copied correctly after $MAX_ATTEMPTS attempts each -- exceeds the " +
          s"tolerance of $MAX_TOLERABLE_ERRORS. See ERROR lines above (first 200) for details.")
    } else if (allErrors.nonEmpty) {
      println(s"WARNING: ${allErrors.length} file(s) out of ${mismatched.length} flagged failed " +
        s"after $MAX_ATTEMPTS attempts each, within the tolerance of $MAX_TOLERABLE_ERRORS -- job " +
        "reports success anyway. See ERROR lines above for the specific file(s).")
    }
  }
}
