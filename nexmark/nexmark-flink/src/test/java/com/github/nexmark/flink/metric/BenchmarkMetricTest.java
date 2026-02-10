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

import org.junit.Assert;
import org.junit.Test;

import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValue;
import static org.junit.Assert.assertEquals;

public class BenchmarkMetricTest {

	@Test
	public void testFormatLongValue() {
		Assert.assertEquals("1.64 M", formatLongValue(1_636_000));
		Assert.assertEquals("1.6 M", formatLongValue(1_600_000));
		Assert.assertEquals("232", formatLongValue(232));
		Assert.assertEquals("23.21 K", formatLongValue(23213));
	}

	@Test
	public void testBenchmarkMetricWithAllFields() {
		long timestamp = 1706284800000L;
		double tps = 150000.0;
		double cpu = 8.5;
		long rss = 1073741824L;      // 1GB
		long vmem = 2147483648L;     // 2GB
		long netRead = 52428800L;    // 50MB
		long netWrite = 10485760L;   // 10MB
		long diskRead = 104857600L;  // 100MB
		long diskWrite = 52428800L;  // 50MB

		BenchmarkMetric metric = new BenchmarkMetric(
				timestamp, tps, cpu, rss, vmem, netRead, netWrite, diskRead, diskWrite);

		assertEquals(timestamp, metric.getTimestamp());
		assertEquals(tps, metric.getTps(), 0.001);
		assertEquals(cpu, metric.getCpu(), 0.001);
		assertEquals(rss, metric.getRss());
		assertEquals(vmem, metric.getVmem());
		assertEquals(netRead, metric.getNetBytesRead());
		assertEquals(netWrite, metric.getNetBytesWritten());
		assertEquals(diskRead, metric.getDiskBytesRead());
		assertEquals(diskWrite, metric.getDiskBytesWritten());

		// Test pretty formatters
		assertEquals("150 K", metric.getPrettyTps());
		assertEquals("1.07 G", metric.getPrettyRss());
		assertEquals("2.15 G", metric.getPrettyVmem());
	}

	@Test
	public void testBenchmarkMetricWithDefaultMemoryAndIo() {
		// Test creating metric with zeros for memory and I/O fields
		long timestamp = System.currentTimeMillis();
		BenchmarkMetric metric = new BenchmarkMetric(timestamp, 150000.0, 8.5, 0, 0, 0, 0, 0, 0);

		assertEquals(150000.0, metric.getTps(), 0.001);
		assertEquals(8.5, metric.getCpu(), 0.001);
		assertEquals(0L, metric.getRss());
		assertEquals(0L, metric.getVmem());
		assertEquals(0L, metric.getNetBytesRead());
		assertEquals(0L, metric.getNetBytesWritten());
		assertEquals(0L, metric.getDiskBytesRead());
		assertEquals(0L, metric.getDiskBytesWritten());
		assertEquals(timestamp, metric.getTimestamp());
	}

	@Test
	public void testBenchmarkMetricEquality() {
		long timestamp = 1706284800000L;
		BenchmarkMetric metric1 = new BenchmarkMetric(
				timestamp, 150000.0, 8.5, 1073741824L, 2147483648L,
				52428800L, 10485760L, 104857600L, 52428800L);
		BenchmarkMetric metric2 = new BenchmarkMetric(
				timestamp, 150000.0, 8.5, 1073741824L, 2147483648L,
				52428800L, 10485760L, 104857600L, 52428800L);

		assertEquals(metric1, metric2);
		assertEquals(metric1.hashCode(), metric2.hashCode());
	}
}
