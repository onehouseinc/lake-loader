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
import org.apache.flink.runtime.net.ConnectionUtils;

import com.github.nexmark.flink.FlinkNexmarkOptions;
import com.github.nexmark.flink.utils.AutoClosableProcess;
import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProcessMetricSender implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessMetricSender.class);
	public static final char DELIMITER = '\n';

	private final String serverHostIp;
	private final int serverPort;
	private final Duration interval;
	private final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
	private InetAddress serverAddress;
	private String localHostIp;
	private ConcurrentHashMap<Integer, ProcfsBasedProcessTree> processTrees;
	private SysInfoLinux sysInfo;

	public ProcessMetricSender(String serverHostIp, int serverPort, Duration interval) {
		this.serverHostIp = serverHostIp;
		this.serverPort = serverPort;
		this.interval = interval;
	}

	public void startClient() throws Exception {
		List<Integer> taskmanagers = getTaskManagerPidList();
		if (taskmanagers.isEmpty()) {
			throw new RuntimeException("There is no Flink TaskManager is running.");
		}
		this.serverAddress = InetAddress.getByName(serverHostIp);
		this.processTrees = new ConcurrentHashMap<>();
		for (Integer pid : taskmanagers) {
			processTrees.put(pid, new ProcfsBasedProcessTree(String.valueOf(pid)));
		}
		LOG.info("Start to monitor process: {}", taskmanagers);
		this.service.scheduleAtFixedRate(
			this::reportProcessMetric,
			0L,
			interval.toMillis(),
			TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() {
		service.shutdownNow();
	}

	private void reportProcessMetric() {
		try (Socket socket = new Socket(serverAddress, serverPort);
			 OutputStream outputStream = socket.getOutputStream()) {
			InetAddress localAddress = ConnectionUtils.findConnectingAddress(
				new InetSocketAddress(serverAddress, serverPort),
				10000L,
				400);
			this.localHostIp = localAddress.getHostAddress();

			List<ProcessMetric> processMetrics = getProcessMetrics();
			String jsonMessage = NexmarkUtils.MAPPER.writeValueAsString(processMetrics);
			outputStream.write(jsonMessage.getBytes(StandardCharsets.UTF_8));
			outputStream.write(DELIMITER);
			LOG.info("Report process metric: {}", jsonMessage);
		} catch (IOException e) {
			LOG.warn("Can't connect to metric server. Skip to report metric for this round.", e);
		} catch (Exception e) {
			LOG.error("Report process metric error.", e);
		}
	}

	private List<ProcessMetric> getProcessMetrics() {
		// Collect system-wide I/O metrics once per host (lazily initialize cached instance)
		if (sysInfo == null) {
			sysInfo = new SysInfoLinux();
		}
		long netBytesRead = sysInfo.getNetworkBytesRead();
		long netBytesWritten = sysInfo.getNetworkBytesWritten();
		long diskBytesRead = sysInfo.getStorageBytesRead();
		long diskBytesWritten = sysInfo.getStorageBytesWritten();

		List<ProcessMetric> processMetrics = new ArrayList<>();
		for (Map.Entry<Integer, ProcfsBasedProcessTree> entry : processTrees.entrySet()) {
			ProcfsBasedProcessTree processTree = entry.getValue();
			processTree.updateProcessTree();
			int pid = entry.getKey();
			double cpuCores = processTree.getCpuUsagePercent() / 100.0;
			long rss = processTree.getRssMemorySize();
			long vmem = processTree.getVirtualMemorySize();
			processMetrics.add(new ProcessMetric(
					localHostIp, pid, cpuCores,
					rss, vmem,
					netBytesRead, netBytesWritten,
					diskBytesRead, diskBytesWritten));
		}
		return processMetrics;
	}

	public static List<Integer> getTaskManagerPidList() throws IOException {
		List<String> javaProcessors = new ArrayList<>();
		AutoClosableProcess
			.create("jps")
			.setStdoutProcessor(javaProcessors::add)
			.runBlocking();
		List<Integer> taskManagers = new ArrayList<>();
		for (String processor : javaProcessors) {
			if (processor.endsWith("TaskManagerRunner")) {
				String pid = processor.split(" ")[0];
				taskManagers.add(Integer.parseInt(pid));
			}
		}
		return taskManagers;
	}

	public static void main(String[] args) throws Exception {
		// start metric servers
		Configuration conf = NexmarkGlobalConfiguration.loadConfiguration();
		String reporterAddress = conf.get(FlinkNexmarkOptions.METRIC_REPORTER_HOST);
		int reporterPort = conf.get(FlinkNexmarkOptions.METRIC_REPORTER_PORT);
		Duration reportInterval = conf.get(FlinkNexmarkOptions.METRIC_MONITOR_INTERVAL);

		ProcessMetricSender sender = new ProcessMetricSender(reporterAddress, reporterPort, reportInterval);
		Runtime.getRuntime().addShutdownHook(new Thread(sender::close));
		sender.startClient();
		sender.service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}
}
