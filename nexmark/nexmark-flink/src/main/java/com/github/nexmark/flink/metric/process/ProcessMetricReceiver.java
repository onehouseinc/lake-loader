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

package com.github.nexmark.flink.metric.process;

import org.apache.flink.configuration.Configuration;

import com.github.nexmark.flink.FlinkNexmarkOptions;
import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.nexmark.flink.metric.process.ProcessMetricSender.DELIMITER;

public class ProcessMetricReceiver implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessMetricReceiver.class);

	/**
	 * Server socket to listen at.
	 */
	private final ServerSocket server;

	/** Stores full ProcessMetric per TaskManager (key: "host:pid"). */
	private final ConcurrentHashMap<String, ProcessMetric> processMetrics = new ConcurrentHashMap<>();

	private final ExecutorService service = Executors.newCachedThreadPool();

	public ProcessMetricReceiver(String host, int port) {
		try {
			InetAddress address = InetAddress.getByName(host);
			server = new ServerSocket(port, 10, address);
		} catch (IOException e) {
			throw new RuntimeException("Could not open socket to receive back process metrics.", e);
		}
	}

	public void runServer() {
		service.submit(this::runServerBlocking);
	}

	public void runServerBlocking() {
		try {
			//noinspection InfiniteLoopStatement
			while (true) {
				Socket socket = server.accept();
				try {
					service.submit(new ServerThread(socket, processMetrics));
				} catch (Exception e) {
					// Executor may be shut down, close socket to prevent leak
					try {
						socket.close();
					} catch (IOException ignored) {
					}
					throw e;
				}
			}
		} catch (IOException e) {
			LOG.error("Failed to start the socket server.", e);
			try {
				server.close();
			} catch (Throwable ignored) {
			}
		}
	}

	public double getTotalCpu() {
		double sumCpu = 0.0;
		int size = 0;
		for (ProcessMetric metric : processMetrics.values()) {
			size++;
			sumCpu += metric.getCpu();
		}
		if (size == 0) {
			LOG.warn("The process metric receiver doesn't receive any metrics.");
		}
		return sumCpu;
	}

	public int getNumberOfTM() {
		return processMetrics.size();
	}

	/** Get total RSS memory across all TaskManagers. */
	public long getTotalRss() {
		long sum = 0;
		for (ProcessMetric metric : processMetrics.values()) {
			sum += metric.getRss();
		}
		return sum;
	}

	/** Get total virtual memory across all TaskManagers. */
	public long getTotalVmem() {
		long sum = 0;
		for (ProcessMetric metric : processMetrics.values()) {
			sum += metric.getVmem();
		}
		return sum;
	}

	/** Get total network bytes read (deduplicated by host since it's system-wide). */
	public long getTotalNetBytesRead() {
		java.util.Map<String, Long> perHost = new java.util.HashMap<>();
		for (ProcessMetric metric : processMetrics.values()) {
			perHost.merge(metric.getHost(), metric.getNetBytesRead(), Math::max);
		}
		return perHost.values().stream().mapToLong(Long::longValue).sum();
	}

	/** Get total network bytes written (deduplicated by host since it's system-wide). */
	public long getTotalNetBytesWritten() {
		java.util.Map<String, Long> perHost = new java.util.HashMap<>();
		for (ProcessMetric metric : processMetrics.values()) {
			perHost.merge(metric.getHost(), metric.getNetBytesWritten(), Math::max);
		}
		return perHost.values().stream().mapToLong(Long::longValue).sum();
	}

	/** Get total disk bytes read (deduplicated by host since it's system-wide). */
	public long getTotalDiskBytesRead() {
		java.util.Map<String, Long> perHost = new java.util.HashMap<>();
		for (ProcessMetric metric : processMetrics.values()) {
			perHost.merge(metric.getHost(), metric.getDiskBytesRead(), Math::max);
		}
		return perHost.values().stream().mapToLong(Long::longValue).sum();
	}

	/** Get total disk bytes written (deduplicated by host since it's system-wide). */
	public long getTotalDiskBytesWritten() {
		java.util.Map<String, Long> perHost = new java.util.HashMap<>();
		for (ProcessMetric metric : processMetrics.values()) {
			perHost.merge(metric.getHost(), metric.getDiskBytesWritten(), Math::max);
		}
		return perHost.values().stream().mapToLong(Long::longValue).sum();
	}

	@Override
	public void close() {
		try {
			server.close();
		} catch (Throwable ignored) {
		}

		service.shutdownNow();
	}

	private static final class ServerThread implements Runnable {

		private final Socket socket;
		private final ConcurrentHashMap<String, ProcessMetric> processMetrics;

		private ServerThread(Socket socket, ConcurrentHashMap<String, ProcessMetric> processMetrics) {
			this.socket = socket;
			this.processMetrics = processMetrics;
		}

		@Override
		public void run() {
			try {
				InputStream inStream = socket.getInputStream();
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				int b;
				while ((b = inStream.read()) >= 0) {
					// buffer until delimiter
					if (b != DELIMITER) {
						buffer.write(b);
					}
					// decode and emit record
					else {
						byte[] bytes = buffer.toByteArray();
						String message = new String(bytes, StandardCharsets.UTF_8);
						LOG.info("Received process metric report: {}", message);
						List<ProcessMetric> receivedMetrics = ProcessMetric.fromJsonArray(message);
						for (ProcessMetric metric : receivedMetrics) {
							processMetrics.put(metric.getHost() + ":" + metric.getPid(), metric);
						}
						buffer.reset();
					}
				}
			} catch (IOException e) {
				LOG.error("Socket server error.", e);
			} finally {
				try {
					socket.close();
				} catch (IOException ex) {
					// ignore
				}
			}
		}
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		// start metric servers
		Configuration conf = NexmarkGlobalConfiguration.loadConfiguration();
		String reporterAddress = conf.getOptional(FlinkNexmarkOptions.METRIC_REPORTER_RECEIVING_HOST)
			.filter(s -> !s.isEmpty())
			.orElseGet(() -> conf.get(FlinkNexmarkOptions.METRIC_REPORTER_HOST));
		int reporterPort = conf.getOptional(FlinkNexmarkOptions.METRIC_REPORTER_RECEIVING_PORT)
			.orElseGet(() -> conf.get(FlinkNexmarkOptions.METRIC_REPORTER_PORT));
		ProcessMetricReceiver processMetricReceiver = new ProcessMetricReceiver(reporterAddress, reporterPort);
		Runtime.getRuntime().addShutdownHook(new Thread(processMetricReceiver::close));
		processMetricReceiver.runServer();
		processMetricReceiver.service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}
}
