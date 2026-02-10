/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink.metric;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.tuple.Tuple2;

import com.github.nexmark.flink.metric.process.ProcessMetricReceiver;
import com.github.nexmark.flink.metric.tps.TpsMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.nexmark.flink.metric.BenchmarkMetric.NUMBER_FORMAT;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatDoubleValue;

/**
 * A reporter to aggregate metrics and report summary results.
 */
public class MetricReporter {

	private static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);

	private final Duration monitorDelay;
	private final Duration monitorInterval;
	private final Duration monitorDuration;
	private final FlinkRestClient flinkRestClient;
	private final ProcessMetricReceiver processMetricReceiver;
	private final List<BenchmarkMetric> metrics;
	private final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
	private volatile Throwable error;

	public MetricReporter(FlinkRestClient flinkRestClient, ProcessMetricReceiver processMetricReceiver, Duration monitorDelay, Duration monitorInterval, Duration monitorDuration) {
		this.monitorDelay = monitorDelay;
		this.monitorInterval = monitorInterval;
		this.monitorDuration = monitorDuration;
		this.flinkRestClient = flinkRestClient;
		this.processMetricReceiver = processMetricReceiver;
		this.metrics = Collections.synchronizedList(new ArrayList<>());
	}

	private void submitMonitorThread(String jobId, long eventsNum) {

		String vertexId;
		String metricName;

		while (true) {
			Tuple2<String, String> jobInfo = getJobInformation(jobId);
			if (jobInfo != null) {
				vertexId = jobInfo.f0;
				metricName = jobInfo.f1;
				break;
			} else {
				// wait for the job startup
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		this.service.scheduleWithFixedDelay(
			new MetricCollector(jobId, vertexId, metricName, eventsNum),
			0L,
			monitorInterval.toMillis(),
			TimeUnit.MILLISECONDS
		);
	}

	private Tuple2<String, String> getJobInformation(String jobId) {
		try {
			String vertexId = flinkRestClient.getSourceVertexId(jobId);
			String metricName = flinkRestClient.getTpsMetricName(jobId, vertexId);
			return Tuple2.of(vertexId, metricName);
		} catch (Exception e) {
			LOG.warn("Job metric is not ready yet.", e);
			return null;
		}
	}

	private void waitFor(Duration duration) {
		Deadline deadline = Deadline.fromNow(duration);
		while (deadline.hasTimeLeft()) {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (error != null) {
				throw new RuntimeException(error);
			}
		}
	}

	private boolean isJobRunning(String jobId) {
		return flinkRestClient.isJobRunning(jobId);
	}

	private void waitForOrJobFinish(String jobId, Duration duration, boolean isKafkaUsed) {
		// The TPS drop to 0 which means job is finished or specific interval for tps mode
		Deadline deadline = Deadline.fromNow(duration);
		while (isJobRunning(jobId) && deadline.hasTimeLeft()) {
            if (isKafkaUsed && jobIsFinished()) break;
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (error != null) {
				throw new RuntimeException(error);
			}
		}
	}

	private void waitForOrJobRunning(String jobId) {
		while (!flinkRestClient.isJobAndAllTasksRunning(jobId)) {
			try {
				Thread.sleep(50L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private boolean jobIsFinished() {
		synchronized (metrics) {
			if (metrics.size() <= 5) {
				return false;
			}
			int lastPos = metrics.size() - 1;
			BenchmarkMetric lastMetric = metrics.get(lastPos);
			if (Double.compare(lastMetric.getTps(), 0.0) == 0) {
				for (int i = 1; i < 5; i++) {
					if (Double.compare(metrics.get(lastPos - i).getTps(), 0.0) != 0) {
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}

	public JobBenchmarkMetric reportMetric(String jobId, long eventsNum, boolean trim, boolean isKafkaUsed) {
		long startTime = System.currentTimeMillis();
		waitForOrJobRunning(jobId);
		long jobStartTime = System.currentTimeMillis();
		System.out.println("Waited for job until running status for " + (jobStartTime - startTime) + "ms.");
		System.out.printf("Monitor metrics after %s seconds.%n", monitorDelay.getSeconds());
		waitFor(monitorDelay);
		if (eventsNum == 0) {
			System.out.printf("Start to monitor metrics for %s seconds.%n", monitorDuration.getSeconds());
		} else {
			System.out.println("Start to monitor metrics until job is finished.");
		}
		submitMonitorThread(jobId, eventsNum);
		// monitorDuration is Long.MAX_VALUE in event number mode
		waitForOrJobFinish(jobId, monitorDuration, isKafkaUsed);

		long endTime = System.currentTimeMillis();

		// cleanup the resource
		this.close();

		if (metrics.isEmpty()) {
			throw new RuntimeException("The metric reporter doesn't collect any metrics.");
		}
		double sumTps = 0.0;
		double sumCpu = 0.0;
		int realMetricSize = metrics.size();

		// If the job finished, the tps will drop to 0, so we need to remove the effect of these metrics on the final result
		if (trim) {
			for (int i = metrics.size() - 1; i >= 0; i--) {
				if (Double.compare(metrics.get(i).getTps(), 0.0) != 0) {
					break;
				} else {
					realMetricSize--;
				}
			}
		}

		List<BenchmarkMetric> realMetrics = metrics.subList(0, realMetricSize);
		for (BenchmarkMetric metric : realMetrics) {
			sumTps += metric.getTps();
			sumCpu += metric.getCpu();
		}

		double avgTps = sumTps / realMetrics.size();
		double avgCpu = sumCpu / realMetrics.size();
		JobBenchmarkMetric metric = new JobBenchmarkMetric(
				avgTps, avgCpu, eventsNum, endTime - jobStartTime, jobStartTime - startTime);

		String message;
		if (eventsNum == 0) {
			message = String.format("Summary Average: Throughput=%s, Cores=%s",
					metric.getPrettyTps(),
					metric.getPrettyCpu());
		} else {
			message = String.format("Summary Average: EventsNum=%s, Cores=%s, Time=%s s, Initialize Time=%s s",
					NUMBER_FORMAT.format(eventsNum),
					metric.getPrettyCpu(),
					formatDoubleValue(metric.getTimeSeconds()),
					formatDoubleValue(metric.getJobInitializedTimeSeconds()));
		}
		System.out.println(message);
		LOG.info(message);
		return metric;
	}

	public void close() {
		service.shutdownNow();
	}

	/**
	 * Get the collected metric samples (for JSON output).
	 * Returns a trimmed copy excluding trailing 0-TPS samples.
	 */
	public List<BenchmarkMetric> getMetricSamples() {
		List<BenchmarkMetric> snapshot;
		synchronized (metrics) {
			snapshot = new ArrayList<>(metrics);
		}
		// Trim trailing 0-TPS samples from the snapshot
		int trimmedSize = snapshot.size();
		for (int i = snapshot.size() - 1; i >= 0; i--) {
			if (Double.compare(snapshot.get(i).getTps(), 0.0) != 0) {
				break;
			} else {
				trimmedSize--;
			}
		}
		// Remove trailing samples from the copy so we don't expose a subList view
		if (trimmedSize < snapshot.size()) {
			snapshot.subList(trimmedSize, snapshot.size()).clear();
		}
		return snapshot;
	}

	private class MetricCollector implements Runnable {
		private final String jobId;
		private final String vertexId;
		private final String metricName;
		private final long eventsNum;

		private MetricCollector(String jobId, String vertexId, String metricName, long eventsNum) {
			this.jobId = jobId;
			this.vertexId = vertexId;
			this.metricName = metricName;
			this.eventsNum = eventsNum;
		}

		@Override
		public void run() {
			try {
				TpsMetric tps = flinkRestClient.getTpsMetric(jobId, vertexId, metricName);
				double cpu = processMetricReceiver.getTotalCpu();
				int tms = processMetricReceiver.getNumberOfTM();
				long rss = processMetricReceiver.getTotalRss();
				long vmem = processMetricReceiver.getTotalVmem();
				long netBytesRead = processMetricReceiver.getTotalNetBytesRead();
				long netBytesWritten = processMetricReceiver.getTotalNetBytesWritten();
				long diskBytesRead = processMetricReceiver.getTotalDiskBytesRead();
				long diskBytesWritten = processMetricReceiver.getTotalDiskBytesWritten();

				BenchmarkMetric metric = new BenchmarkMetric(
						System.currentTimeMillis(),
						tps.getSum(),
						cpu,
						rss,
						vmem,
						netBytesRead,
						netBytesWritten,
						diskBytesRead,
						diskBytesWritten);
				// it's thread-safe to update metrics
				metrics.add(metric);
				// logging
				String message = eventsNum == 0 ?
						String.format("Current Throughput=%s, Cores=%s, RSS=%s (%s TMs)",
								metric.getPrettyTps(), metric.getPrettyCpu(), metric.getPrettyRss(), tms) :
						String.format("Current Cores=%s, RSS=%s (%s TMs)", metric.getPrettyCpu(), metric.getPrettyRss(), tms);
				System.out.println(message);
				LOG.info(message);
			} catch (Exception e) {
				error = e;
			}
		}
	}
}
