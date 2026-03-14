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

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.model.Person;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RowDataEventDeserializer} partition key and distribution modes.
 */
public class RowDataEventDeserializerTest {

	private static final long EPOCH_2025_02_27 = LocalDate.of(2025, 2, 27).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
	private static final long EPOCH_2025_02_26 = LocalDate.of(2025, 2, 26).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
	private static final long EPOCH_2025_02_25 = LocalDate.of(2025, 2, 25).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

	@Test
	public void testNoPartitionColumnWhenConfigNull() {
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(null);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		assertEquals(4, row.getArity());
	}

	@Test
	public void testNoPartitionColumnWhenPartitionKeyFieldEmpty() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		assertEquals(4, row.getArity());
	}

	@Test
	public void testPartitionColumnFallbackFromEventTimestamp() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionValues = "";
		conf.partitionNumber = 0;
		conf.partitionDistribution = "";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Instant eventTime = LocalDate.of(2025, 2, 27).atStartOfDay(ZoneOffset.UTC).toInstant();
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", eventTime, ""));
		RowData row = deserializer.deserialize(event);
		assertEquals(5, row.getArity());
		assertEquals(EPOCH_2025_02_27, row.getLong(4));
	}

	@Test
	public void testLatestModeAllToFirstPartition() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.LATEST;
		conf.partitionValues = "2025-02-27,2025-02-26";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event personEvent = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		Event auctionEvent = new Event(new Auction(2L, "i", "d", 1L, 2L, Instant.EPOCH, Instant.EPOCH, 1L, 1L, ""));
		Event bidEvent = new Event(new Bid(1L, 1L, 10L, "ch", "url", Instant.EPOCH, ""));
		assertEquals(EPOCH_2025_02_27, deserializer.deserialize(personEvent).getLong(4));
		assertEquals(EPOCH_2025_02_27, deserializer.deserialize(auctionEvent).getLong(4));
		assertEquals(EPOCH_2025_02_27, deserializer.deserialize(bidEvent).getLong(4));
	}

	@Test
	public void testUniformModeSpreadAcrossPartitions() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.UNIFORM;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		long[] expected = { EPOCH_2025_02_27, EPOCH_2025_02_26, EPOCH_2025_02_25 };
		for (int i = 0; i < 3; i++) {
			Event event = new Event(new Person((long) i, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
			RowData row = deserializer.deserialize(event);
			long partitionMs = row.getLong(4);
			assertTrue(partitionMs == expected[0] || partitionMs == expected[1] || partitionMs == expected[2]);
		}
	}

	@Test
	public void testRandomModeSpreadAcrossPartitions() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.RANDOM;
		conf.partitionValues = "2025-02-27,2025-02-26";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		long partitionMs = row.getLong(4);
		assertTrue(partitionMs == EPOCH_2025_02_27 || partitionMs == EPOCH_2025_02_26);
	}

	@Test
	public void testCustomModeValueEqualsWeight() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.CUSTOM;
		conf.partitionDistribution = "2025-02-27=80,2025-02-26=15,2025-02-25=5";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		long partitionMs = row.getLong(4);
		assertTrue(partitionMs == EPOCH_2025_02_27 || partitionMs == EPOCH_2025_02_26 || partitionMs == EPOCH_2025_02_25);
	}

	@Test
	public void testCustomModeWeightsOnly() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.CUSTOM;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		conf.partitionDistribution = "80,15,5";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		long partitionMs = row.getLong(4);
		assertTrue(partitionMs == EPOCH_2025_02_27 || partitionMs == EPOCH_2025_02_26 || partitionMs == EPOCH_2025_02_25);
	}

	@Test
	public void testSkewedModeZipfian() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.SKEWED;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		conf.partitionDistribution = "zipfian";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		long partitionMs = row.getLong(4);
		assertTrue(partitionMs == EPOCH_2025_02_27 || partitionMs == EPOCH_2025_02_26 || partitionMs == EPOCH_2025_02_25);
	}

	@Test
	public void testSkewedModeZipfianWithExponent() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.SKEWED;
		conf.partitionValues = "2025-02-27,2025-02-26";
		conf.partitionDistribution = "zipfian:1.5";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		long partitionMs = row.getLong(4);
		assertTrue(partitionMs == EPOCH_2025_02_27 || partitionMs == EPOCH_2025_02_26);
	}

	@Test
	public void testPartitionValueIsEpochMilliseconds() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.LATEST;
		conf.partitionValues = "2025-02-27";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		assertEquals(5, row.getArity());
		assertEquals(EPOCH_2025_02_27, row.getLong(4));
	}

	@Test
	public void testBackwardCompatibilitySkewedWithWeightsTreatedAsCustom() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.SKEWED;
		conf.partitionValues = "2025-02-27,2025-02-26";
		conf.partitionDistribution = "70,30";
		GeneratorConfig config = new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
		RowDataEventDeserializer deserializer = new RowDataEventDeserializer(config);
		Event event = new Event(new Person(1L, "a", "b", "c", "d", "e", Instant.EPOCH, ""));
		RowData row = deserializer.deserialize(event);
		long partitionMs = row.getLong(4);
		assertTrue(partitionMs == EPOCH_2025_02_27 || partitionMs == EPOCH_2025_02_26);
	}
}
