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

package com.github.nexmark.flink.source;

/**
 * How partition key values are assigned when {@link NexmarkConfiguration#partitionKeyField} is set.
 */
public enum PartitionDistributionMode {

	/**
	 * Records are spread evenly across all partition values (from {@link NexmarkConfiguration#partitionValues}).
	 * Simulates a steady state where every part of the data lake receives a balanced amount of data.
	 */
	UNIFORM,

	/**
	 * 100% of records go to the active/latest partition (e.g. "today").
	 * Simulates events tables (logs, clickstreams) where data always belongs to the current partition.
	 */
	LATEST,

	/**
	 * Records are scattered across partitions based on a deterministic hash of the event.
	 * Simulates dimension tables where updates can happen to any partition.
	 */
	RANDOM,

	/**
	 * Partition values follow a Zipfian (power-law) skew via {@link NexmarkConfiguration#partitionDistribution}
	 * (e.g. "zipfian" or "zipfian:1.5"). First partition gets the most data, then decay by rank.
	 */
	SKEWED,

	/**
	 * Partition values follow a custom weighted distribution in {@link NexmarkConfiguration#partitionDistribution}:
	 * either "value1=weight1,value2=weight2,..." (e.g. "2025-02-27=80,2025-02-26=15") or weights-only in partition order
	 * (e.g. "80,15,5,0,0,0,0" when using {@link NexmarkConfiguration#partitionNumber}).
	 */
	CUSTOM
}
