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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import com.github.nexmark.flink.utils.NexmarkUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProcessMetric {

	private static final String FIELD_NAME_HOST = "host";
	private static final String FIELD_NAME_PID = "pid";
	private static final String FIELD_NAME_CPU = "cpu";
	private static final String FIELD_NAME_RSS = "rss";
	private static final String FIELD_NAME_VMEM = "vmem";
	private static final String FIELD_NAME_NET_BYTES_READ = "netBytesRead";
	private static final String FIELD_NAME_NET_BYTES_WRITTEN = "netBytesWritten";
	private static final String FIELD_NAME_DISK_BYTES_READ = "diskBytesRead";
	private static final String FIELD_NAME_DISK_BYTES_WRITTEN = "diskBytesWritten";

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(value = FIELD_NAME_HOST, required = true)
	private final String host;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(value = FIELD_NAME_PID, required = true)
	private final int pid;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(value = FIELD_NAME_CPU, required = true)
	private final double cpu;

	@JsonProperty(value = FIELD_NAME_RSS)
	private final long rss;

	@JsonProperty(value = FIELD_NAME_VMEM)
	private final long vmem;

	@JsonProperty(value = FIELD_NAME_NET_BYTES_READ)
	private final long netBytesRead;

	@JsonProperty(value = FIELD_NAME_NET_BYTES_WRITTEN)
	private final long netBytesWritten;

	@JsonProperty(value = FIELD_NAME_DISK_BYTES_READ)
	private final long diskBytesRead;

	@JsonProperty(value = FIELD_NAME_DISK_BYTES_WRITTEN)
	private final long diskBytesWritten;

	@JsonCreator
	public ProcessMetric(
			@Nullable @JsonProperty(FIELD_NAME_HOST) String host,
			@JsonProperty(FIELD_NAME_PID) int pid,
			@JsonProperty(FIELD_NAME_CPU) double cpu,
			@JsonProperty(FIELD_NAME_RSS) long rss,
			@JsonProperty(FIELD_NAME_VMEM) long vmem,
			@JsonProperty(FIELD_NAME_NET_BYTES_READ) long netBytesRead,
			@JsonProperty(FIELD_NAME_NET_BYTES_WRITTEN) long netBytesWritten,
			@JsonProperty(FIELD_NAME_DISK_BYTES_READ) long diskBytesRead,
			@JsonProperty(FIELD_NAME_DISK_BYTES_WRITTEN) long diskBytesWritten) {
		this.host = host;
		this.pid = pid;
		this.cpu = cpu;
		this.rss = rss;
		this.vmem = vmem;
		this.netBytesRead = netBytesRead;
		this.netBytesWritten = netBytesWritten;
		this.diskBytesRead = diskBytesRead;
		this.diskBytesWritten = diskBytesWritten;
	}

	@JsonIgnore
	public String getHost() {
		return host;
	}

	@JsonIgnore
	public int getPid() {
		return pid;
	}

	@JsonIgnore
	public double getCpu() {
		return cpu;
	}

	@JsonIgnore
	public long getRss() {
		return rss;
	}

	@JsonIgnore
	public long getVmem() {
		return vmem;
	}

	@JsonIgnore
	public long getNetBytesRead() {
		return netBytesRead;
	}

	@JsonIgnore
	public long getNetBytesWritten() {
		return netBytesWritten;
	}

	@JsonIgnore
	public long getDiskBytesRead() {
		return diskBytesRead;
	}

	@JsonIgnore
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
		ProcessMetric processMetric = (ProcessMetric) o;
		return Double.compare(processMetric.cpu, cpu) == 0 &&
			pid == processMetric.pid &&
			rss == processMetric.rss &&
			vmem == processMetric.vmem &&
			netBytesRead == processMetric.netBytesRead &&
			netBytesWritten == processMetric.netBytesWritten &&
			diskBytesRead == processMetric.diskBytesRead &&
			diskBytesWritten == processMetric.diskBytesWritten &&
			Objects.equals(host, processMetric.host);
	}

	@Override
	public int hashCode() {
		return Objects.hash(host, pid, cpu, rss, vmem, netBytesRead, netBytesWritten, diskBytesRead, diskBytesWritten);
	}

	@Override
	public String toString() {
		try {
			return NexmarkUtils.MAPPER.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public static List<ProcessMetric> fromJsonArray(String json) {
		try {
			ArrayNode arrayNode = (ArrayNode) NexmarkUtils.MAPPER.readTree(json);
			List<ProcessMetric> expected = new ArrayList<>();
			for (JsonNode jsonNode : arrayNode) {
				expected.add(NexmarkUtils.MAPPER.convertValue(jsonNode, ProcessMetric.class));
			}
			return expected;
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}
