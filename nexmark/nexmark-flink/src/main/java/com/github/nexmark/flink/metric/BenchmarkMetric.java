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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class BenchmarkMetric {
	private final long timestamp;
	private final double tps;
	private final double cpu;
	private final long rss;
	private final long vmem;
	private final long netBytesRead;
	private final long netBytesWritten;
	private final long diskBytesRead;
	private final long diskBytesWritten;

	/** Full constructor with all metrics. */
	public BenchmarkMetric(
			long timestamp,
			double tps,
			double cpu,
			long rss,
			long vmem,
			long netBytesRead,
			long netBytesWritten,
			long diskBytesRead,
			long diskBytesWritten) {
		this.timestamp = timestamp;
		this.tps = tps;
		this.cpu = cpu;
		this.rss = rss;
		this.vmem = vmem;
		this.netBytesRead = netBytesRead;
		this.netBytesWritten = netBytesWritten;
		this.diskBytesRead = diskBytesRead;
		this.diskBytesWritten = diskBytesWritten;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public double getTps() {
		return tps;
	}

	public String getPrettyTps() {
		return formatLongValue((long) tps);
	}

	public double getCpu() {
		return cpu;
	}

	public String getPrettyCpu() {
		return NUMBER_FORMAT.format(cpu);
	}

	public long getRss() {
		return rss;
	}

	public String getPrettyRss() {
		return formatLongValue(rss);
	}

	public long getVmem() {
		return vmem;
	}

	public String getPrettyVmem() {
		return formatLongValue(vmem);
	}

	public long getNetBytesRead() {
		return netBytesRead;
	}

	public long getNetBytesWritten() {
		return netBytesWritten;
	}

	public long getDiskBytesRead() {
		return diskBytesRead;
	}

	public long getDiskBytesWritten() {
		return diskBytesWritten;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BenchmarkMetric that = (BenchmarkMetric) o;
		return timestamp == that.timestamp &&
			Double.compare(that.tps, tps) == 0 &&
			Double.compare(that.cpu, cpu) == 0 &&
			rss == that.rss &&
			vmem == that.vmem &&
			netBytesRead == that.netBytesRead &&
			netBytesWritten == that.netBytesWritten &&
			diskBytesRead == that.diskBytesRead &&
			diskBytesWritten == that.diskBytesWritten;
	}

	@Override
	public int hashCode() {
		return Objects.hash(timestamp, tps, cpu, rss, vmem, netBytesRead, netBytesWritten, diskBytesRead, diskBytesWritten);
	}

	@Override
	public String toString() {
		return "BenchmarkMetric{" +
			"timestamp=" + timestamp +
			", tps=" + tps +
			", cpu=" + cpu +
			", rss=" + rss +
			", vmem=" + vmem +
			", netBytesRead=" + netBytesRead +
			", netBytesWritten=" + netBytesWritten +
			", diskBytesRead=" + diskBytesRead +
			", diskBytesWritten=" + diskBytesWritten +
			'}';
	}


	// -------------------------------------------------------------------------------------------
	// Pretty Utilities
	// -------------------------------------------------------------------------------------------
	public static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	private static final NavigableMap<Long, String> SUFFIXES = new TreeMap<>();
	static {
		SUFFIXES.put(1_000L, "K");
		SUFFIXES.put(1_000_000L, "M");
		SUFFIXES.put(1_000_000_000L, "G");
		SUFFIXES.put(1_000_000_000_000L, "T");
		SUFFIXES.put(1_000_000_000_000_000L, "P");
		SUFFIXES.put(1_000_000_000_000_000_000L, "E");
		NUMBER_FORMAT.setMaximumFractionDigits(2);
	}

	public static String formatLongValuePerSecond(long value) {
		return formatLongValue(value) + "/s";
	}

	public static String formatLongValue(long value) {
		//Long.MIN_VALUE == -Long.MIN_VALUE so we need an adjustment here
		if (value == Long.MIN_VALUE) return formatLongValue(Long.MIN_VALUE + 1);
		if (value < 0) return "-" + formatLongValue(-value);
		if (value < 1000) return Long.toString(value); //deal with easy case

		Map.Entry<Long, String> e = SUFFIXES.floorEntry(value);
		Long divideBy = e.getKey();
		String suffix = e.getValue();

		DecimalFormat format = new DecimalFormat("0.##");
		return format.format(value / (double) divideBy) + " " + suffix;
	}

	public static String formatDoubleValue(double value) {
		return String.format("%.3f", value);
	}
}
