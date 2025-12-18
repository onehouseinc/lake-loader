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

package ai.onehouse.lakeloader.compaction

import ai.onehouse.lakeloader.configs.StorageFormat
import ai.onehouse.lakeloader.configs.StorageFormat.{Delta, Hudi, Iceberg}
import ai.onehouse.lakeloader.metrics.MetricsCollector
import ai.onehouse.lakeloader.utils.SparkUtils.executeSparkSql
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.spark.sql.SparkSession

import java.time.Instant
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.JavaConverters._

class AsyncCompactionService(
    spark: SparkSession,
    format: StorageFormat,
    tablePath: String,
    tableName: String,
    metricsCollector: MetricsCollector,
    writeOptions: Map[String, String] = Map.empty,
    compactionFrequencyCommits: Int = 3,
    retryDelayMinutes: Int = 1) {

  private val executor: ExecutorService = Executors.newSingleThreadExecutor()
  private val retryScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  private val isServiceRunning = new AtomicBoolean(false)
  private val isCompactionInProgress = new AtomicBoolean(false)
  private val compactionRunId = new AtomicInteger(0)
  // Track commits since last compaction for Iceberg/Delta
  private val commitsSinceLastCompaction = new AtomicInteger(0)

  def start(): Unit = {
    if (isServiceRunning.compareAndSet(false, true)) {
      println(s"[AsyncCompaction] Starting commit-based compaction service for $format at $tablePath (frequency: every $compactionFrequencyCommits commits)")
    }
  }

  def notifyNewCommit(): Unit = {
    val commits = commitsSinceLastCompaction.incrementAndGet()
    if (!isServiceRunning.get()) {
      return
    }
    println(s"[AsyncCompaction] New commit detected. Commits since last compaction: $commits")

    // Submit compaction check to background thread
    executor.submit(new Runnable {
      def run(): Unit = runCompactionIfNeeded()
    })
  }

  private def runCompactionIfNeeded(): Unit = {
    // Ensure only one compaction runs at a time
    if (!isCompactionInProgress.compareAndSet(false, true)) {
      println("[AsyncCompaction] Compaction already in progress. Skipping.")
      return
    }

    try {
      val startTime = Instant.now()

      val runId = compactionRunId.incrementAndGet()
      try {
        val compactionRan = runCompaction()
        if (compactionRan) {
          val endTime = Instant.now()
          val metrics =
            metricsCollector.recordCompaction(runId, format.asString, startTime, endTime, success = true)
          println(s"[AsyncCompaction] Compaction completed successfully. $metrics")
        } else {
          compactionRunId.decrementAndGet()
        }
      } catch {
        case e: Exception =>
          val endTime = Instant.now()
          metricsCollector.recordCompaction(
            runId,
            format.asString,
            startTime,
            endTime,
            success = false,
            Some(e.getMessage))
          println(s"[AsyncCompaction] Compaction failed: ${e.getMessage}")
          e.printStackTrace()
          throw e
          // Schedule a retry
          scheduleRetry()
      }
    } finally {
      isCompactionInProgress.set(false)
    }
  }

  private def scheduleRetry(): Unit = {
    if (isServiceRunning.get()) {
      println(s"[AsyncCompaction] Scheduling retry in $retryDelayMinutes minute(s)...")
      retryScheduler.schedule(
        new Runnable { def run(): Unit = runCompactionIfNeeded() },
        retryDelayMinutes.toLong,
        TimeUnit.MINUTES)
    }
  }

  private def runCompaction(): Boolean = {
    format match {
      case Hudi =>
        // Hudi schedules compaction internally, we just check for pending instants
        runHudiCompaction()
      case Iceberg =>
        // Only run compaction after N commits
        if (commitsSinceLastCompaction.get() >= compactionFrequencyCommits) {
          runIcebergCompaction()
          commitsSinceLastCompaction.set(0)
          true
        } else {
          println(s"[AsyncCompaction] Skipping Iceberg compaction. Commits: ${commitsSinceLastCompaction.get()}/$compactionFrequencyCommits")
          false
        }
      case Delta =>
        // Only run compaction after N commits
        if (commitsSinceLastCompaction.get() >= compactionFrequencyCommits) {
          runDeltaCompaction()
          commitsSinceLastCompaction.set(0)
          true
        } else {
          println(s"[AsyncCompaction] Skipping Delta compaction. Commits: ${commitsSinceLastCompaction.get()}/$compactionFrequencyCommits")
          false
        }
      case _ =>
        println(s"[AsyncCompaction] Compaction not supported for format: $format")
        false
    }
  }

  private def runHudiCompaction(): Boolean = {
    // Find the first pending compaction instant from the timeline
    val pendingCompactionInstant = getFirstPendingCompactionInstant

    pendingCompactionInstant match {
      case Some(instant) =>
        val optionsStr = writeOptions.map { case (k, v) => s"$k=$v" }.mkString(",")
        val compactionSql =
          s"CALL run_compaction(op => 'run', path => '$tablePath', timestamp => $instant, options => '$optionsStr')"
        println(s"[AsyncCompaction] Running Hudi compaction for instant $instant")
        executeSparkSql(spark, compactionSql)
        true
      case None =>
        println("[AsyncCompaction] No pending compaction instants found.")
        false
    }
  }

  private def getFirstPendingCompactionInstant: Option[String] = {
    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val storageConf = new HadoopStorageConfiguration(hadoopConf)
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf)
        .setBasePath(tablePath)
        .build()

      val timeline = metaClient.getActiveTimeline
      val pendingCompactions = timeline.filterPendingCompactionTimeline().getInstants.iterator().asScala.toList

      if (pendingCompactions.nonEmpty) {
        val firstInstant = pendingCompactions.head.requestedTime
        println(s"[AsyncCompaction] Found ${pendingCompactions.size} pending compaction(s). First: $firstInstant")
        Some(firstInstant)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        println(s"[AsyncCompaction] Error reading Hudi timeline: ${e.getMessage}")
        None
    }
  }

  private def runIcebergCompaction(): Unit = {
    val catalogName = tableName.split("\\.").headOption.getOrElse("spark_catalog")
    val compactionSql = s"CALL $catalogName.system.rewrite_data_files(table => '$tableName')"
    println(s"[AsyncCompaction] Running Iceberg compaction: $compactionSql")
    executeSparkSql(spark, compactionSql)
  }

  private def runDeltaCompaction(): Unit = {
    val optimizeSql = s"OPTIMIZE delta.`$tablePath`"
    println(s"[AsyncCompaction] Running Delta compaction: $optimizeSql")
    executeSparkSql(spark, optimizeSql)
  }

  def shutdown(runFinalCompaction: Boolean = true): Unit = {
    println("[AsyncCompaction] Shutting down compaction service...")

    if (isServiceRunning.compareAndSet(true, false)) {
      // Stop accepting new retry tasks
      retryScheduler.shutdown()

      if (runFinalCompaction) {
        // Wait for any in-progress compaction to finish
        while (isCompactionInProgress.get()) {
          println("[AsyncCompaction] Waiting for in-progress compaction to complete...")
          Thread.sleep(30000)
        }
        println("[AsyncCompaction] Running final compaction before shutdown...")
        runCompactionIfNeeded()
      }

      executor.shutdown()
      println("[AsyncCompaction] Compaction service stopped.")
    }
  }
}
