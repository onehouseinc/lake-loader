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

package ai.onehouse.lakeloader.metrics

import java.time.{Duration, Instant}
import scala.collection.mutable.ListBuffer

case class RoundTiming(
    startTime: Instant,
    endTime: Instant,
    durationMs: Long) {
  override def toString: String =
    s"RoundTiming(start=$startTime, end=$endTime, duration=${durationMs}ms)"
}

case class BatchMetrics(
    roundNo: Int,
    startTime: Instant,
    endTime: Instant,
    durationMs: Long) {
  override def toString: String =
    s"BatchMetrics(round=$roundNo, start=$startTime, end=$endTime, duration=${durationMs}ms)"
}

case class CompactionMetrics(
    runId: Int,
    format: String,
    startTime: Instant,
    endTime: Instant,
    durationMs: Long,
    success: Boolean,
    errorMessage: Option[String] = None) {
  override def toString: String = {
    val status = if (success) "SUCCESS" else s"FAILED: ${errorMessage.getOrElse("unknown")}"
    s"CompactionMetrics(runId=$runId, format=$format, start=$startTime, end=$endTime, duration=${durationMs}ms, status=$status)"
  }
}

class MetricsCollector {
  private val batchMetrics = ListBuffer[BatchMetrics]()
  private val compactionMetrics = ListBuffer[CompactionMetrics]()

  def recordBatch(roundNo: Int, startTime: Instant, endTime: Instant): BatchMetrics = {
    val metrics = BatchMetrics(
      roundNo = roundNo,
      startTime = startTime,
      endTime = endTime,
      durationMs = Duration.between(startTime, endTime).toMillis)
    batchMetrics.synchronized {
      batchMetrics += metrics
    }
    metrics
  }

  def recordCompaction(
      runId: Int,
      format: String,
      startTime: Instant,
      endTime: Instant,
      success: Boolean,
      errorMessage: Option[String] = None): CompactionMetrics = {
    val metrics = CompactionMetrics(
      runId = runId,
      format = format,
      startTime = startTime,
      endTime = endTime,
      durationMs = Duration.between(startTime, endTime).toMillis,
      success = success,
      errorMessage = errorMessage)
    compactionMetrics.synchronized {
      compactionMetrics += metrics
    }
    metrics
  }

  def getBatchMetrics: List[BatchMetrics] = batchMetrics.synchronized { batchMetrics.toList }
  def getCompactionMetrics: List[CompactionMetrics] =
    compactionMetrics.synchronized { compactionMetrics.toList }

  def printSummary(): Unit = {
    val sep = "=" * 60
    val batchList = getBatchMetrics
    val compactionList = getCompactionMetrics

    val batchSummary = if (batchList.nonEmpty) {
      val total = batchList.map(_.durationMs).sum
      val avg = total / batchList.size
      s"""BATCH METRICS (${batchList.size} rounds):
         |${batchList.map(m => s"  Round ${m.roundNo}: ${m.durationMs}ms ${m.startTime.toEpochMilli} ${m.endTime.toEpochMilli}").mkString("\n")}
         |  Total batch time: ${total}ms
         |  Avg batch time: ${avg}ms""".stripMargin
    } else {
      "BATCH METRICS: No batches recorded"
    }

    val compactionSummary = if (compactionList.nonEmpty) {
      val total = compactionList.map(_.durationMs).sum
      val successCount = compactionList.count(_.success)
      s"""COMPACTION METRICS (${compactionList.size} runs):
         |${compactionList.map(m => s"  Run ${m.runId}: ${m.durationMs}ms ${m.startTime.toEpochMilli} ${m.endTime.toEpochMilli} (${if (m.success) "SUCCESS" else "FAILED"})").mkString("\n")}
         |  Total compaction time: ${total}ms
         |  Successful runs: $successCount""".stripMargin
    } else {
      "COMPACTION METRICS: No compaction runs recorded"
    }

    println(
      s"""
         |$sep
         |TIMING METRICS SUMMARY
         |$sep
         |
         |$batchSummary
         |
         |$compactionSummary
         |$sep
         |""".stripMargin)
  }
}
