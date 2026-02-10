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

import com.github.nexmark.flink.metric.process.ProcessMetric;
import com.github.nexmark.flink.metric.process.ProcessMetricReceiver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ProcessMetricReceiver} aggregation methods.
 */
public class ProcessMetricReceiverTest {

	private ProcessMetricReceiver receiver;
	private ConcurrentHashMap<String, ProcessMetric> metricsMap;

	@Before
	public void setUp() throws Exception {
		// Use an ephemeral port to avoid conflicts in parallel/CI test runs
		receiver = new ProcessMetricReceiver("localhost", 0);

		// Access the private processMetrics map via reflection for testing
		Field metricsField = ProcessMetricReceiver.class.getDeclaredField("processMetrics");
		metricsField.setAccessible(true);
		@SuppressWarnings("unchecked")
		ConcurrentHashMap<String, ProcessMetric> map =
				(ConcurrentHashMap<String, ProcessMetric>) metricsField.get(receiver);
		metricsMap = map;
	}

	@After
	public void tearDown() {
		if (receiver != null) {
			receiver.close();
		}
	}

	/** Test convenience method to create a ProcessMetric with only CPU data. */
	private static ProcessMetric createProcessMetric(String host, int pid, double cpu) {
		return new ProcessMetric(host, pid, cpu, 0, 0, 0, 0, 0, 0);
	}

	@Test
	public void testGetTotalCpu() {
		// Add metrics from two TaskManagers on the same host
		metricsMap.put("host1:1001", createProcessMetric("host1", 1001, 2.5));
		metricsMap.put("host1:1002", createProcessMetric("host1", 1002, 3.5));

		assertEquals(6.0, receiver.getTotalCpu(), 0.001);
		assertEquals(2, receiver.getNumberOfTM());
	}

	@Test
	public void testGetTotalRssAndVmem() {
		// Add metrics with memory info
		metricsMap.put("host1:1001", new ProcessMetric(
				"host1", 1001, 2.5,
				1073741824L, 2147483648L,  // 1GB RSS, 2GB VMEM
				0, 0, 0, 0));
		metricsMap.put("host1:1002", new ProcessMetric(
				"host1", 1002, 3.5,
				2147483648L, 4294967296L,  // 2GB RSS, 4GB VMEM
				0, 0, 0, 0));

		// RSS and VMEM should sum across all TMs
		assertEquals(3221225472L, receiver.getTotalRss());   // 1GB + 2GB = 3GB
		assertEquals(6442450944L, receiver.getTotalVmem());  // 2GB + 4GB = 6GB
	}

	@Test
	public void testGetTotalNetworkIoDeduplication() {
		// Network I/O is system-wide, so should be deduplicated by host
		// Two TMs on the same host should only count the host's I/O once
		metricsMap.put("host1:1001", new ProcessMetric(
				"host1", 1001, 2.5,
				1073741824L, 2147483648L,
				52428800L, 10485760L,   // 50MB read, 10MB write
				0, 0));
		metricsMap.put("host1:1002", new ProcessMetric(
				"host1", 1002, 3.5,
				2147483648L, 4294967296L,
				52428800L, 10485760L,   // Same values (system-wide)
				0, 0));

		// Should only count once since both TMs are on same host
		assertEquals(52428800L, receiver.getTotalNetBytesRead());
		assertEquals(10485760L, receiver.getTotalNetBytesWritten());
	}

	@Test
	public void testGetTotalNetworkIoMultipleHosts() {
		// Network I/O from different hosts should sum
		metricsMap.put("host1:1001", new ProcessMetric(
				"host1", 1001, 2.5,
				1073741824L, 2147483648L,
				52428800L, 10485760L,     // 50MB read, 10MB write
				0, 0));
		metricsMap.put("host2:2001", new ProcessMetric(
				"host2", 2001, 3.5,
				2147483648L, 4294967296L,
				104857600L, 20971520L,    // 100MB read, 20MB write
				0, 0));

		// Should sum across different hosts
		assertEquals(157286400L, receiver.getTotalNetBytesRead());   // 50MB + 100MB
		assertEquals(31457280L, receiver.getTotalNetBytesWritten()); // 10MB + 20MB
	}

	@Test
	public void testGetTotalDiskIoDeduplication() {
		// Disk I/O is system-wide, so should be deduplicated by host
		metricsMap.put("host1:1001", new ProcessMetric(
				"host1", 1001, 2.5,
				1073741824L, 2147483648L,
				0, 0,
				104857600L, 52428800L));  // 100MB read, 50MB write
		metricsMap.put("host1:1002", new ProcessMetric(
				"host1", 1002, 3.5,
				2147483648L, 4294967296L,
				0, 0,
				104857600L, 52428800L));  // Same values (system-wide)

		// Should only count once since both TMs are on same host
		assertEquals(104857600L, receiver.getTotalDiskBytesRead());
		assertEquals(52428800L, receiver.getTotalDiskBytesWritten());
	}

	@Test
	public void testGetTotalDiskIoMultipleHosts() {
		// Disk I/O from different hosts should sum
		metricsMap.put("host1:1001", new ProcessMetric(
				"host1", 1001, 2.5,
				1073741824L, 2147483648L,
				0, 0,
				104857600L, 52428800L));    // 100MB read, 50MB write
		metricsMap.put("host2:2001", new ProcessMetric(
				"host2", 2001, 3.5,
				2147483648L, 4294967296L,
				0, 0,
				209715200L, 104857600L));   // 200MB read, 100MB write

		// Should sum across different hosts
		assertEquals(314572800L, receiver.getTotalDiskBytesRead());   // 100MB + 200MB
		assertEquals(157286400L, receiver.getTotalDiskBytesWritten()); // 50MB + 100MB
	}

	@Test
	public void testEmptyMetrics() {
		// All aggregation methods should handle empty map gracefully
		assertEquals(0.0, receiver.getTotalCpu(), 0.001);
		assertEquals(0, receiver.getNumberOfTM());
		assertEquals(0L, receiver.getTotalRss());
		assertEquals(0L, receiver.getTotalVmem());
		assertEquals(0L, receiver.getTotalNetBytesRead());
		assertEquals(0L, receiver.getTotalNetBytesWritten());
		assertEquals(0L, receiver.getTotalDiskBytesRead());
		assertEquals(0L, receiver.getTotalDiskBytesWritten());
	}
}
