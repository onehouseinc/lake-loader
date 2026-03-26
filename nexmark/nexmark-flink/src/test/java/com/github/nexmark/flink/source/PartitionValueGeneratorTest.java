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
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link PartitionValueGenerator} covering all distribution modes and edge cases.
 */
public class PartitionValueGeneratorTest {

	private static final long EPOCH_2025_02_27 = LocalDate.of(2025, 2, 27).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
	private static final long EPOCH_2025_02_26 = LocalDate.of(2025, 2, 26).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
	private static final long EPOCH_2025_02_25 = LocalDate.of(2025, 2, 25).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

	private static GeneratorConfig configWith(NexmarkConfiguration conf) {
		return new GeneratorConfig(conf, System.currentTimeMillis(), 1, 100, -1L, 1);
	}

	private static Event personEvent(long id, Instant time) {
		return new Event(new Person(id, "a", "b", "c", "d", "e", time, ""));
	}

	// -------------------------------------------------------------------------
	// Fallback behaviour
	// -------------------------------------------------------------------------

	@Test
	public void testFallbackToEventTimestampWhenNoPartitionValues() {
		// partition key field set but no values/number configured → derive from event dateTime
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionValues = "";
		conf.partitionNumber = 0;
		conf.partitionDistribution = "";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		Instant eventTime = LocalDate.of(2025, 2, 27).atStartOfDay(ZoneOffset.UTC).toInstant();
		Event event = personEvent(1L, eventTime);
		assertEquals(EPOCH_2025_02_27, gen.computePartitionValueMs(event));
	}

	@Test
	public void testFallbackTruncatesNonMidnightTimestampToStartOfDay() {
		// event at noon UTC must still map to start of that day
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		// noon on 2025-02-27 UTC
		Instant noon = LocalDate.of(2025, 2, 27).atStartOfDay(ZoneOffset.UTC).plusSeconds(43200).toInstant();
		Event event = personEvent(1L, noon);
		assertEquals(EPOCH_2025_02_27, gen.computePartitionValueMs(event));
	}

	@Test
	public void testFallbackUsesAuctionTimestamp() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		Instant eventTime = LocalDate.of(2025, 2, 26).atStartOfDay(ZoneOffset.UTC).toInstant();
		Event event = new Event(new Auction(1L, "i", "d", 1L, 2L, eventTime, eventTime, 1L, 1L, ""));
		assertEquals(EPOCH_2025_02_26, gen.computePartitionValueMs(event));
	}

	@Test
	public void testFallbackUsesBidTimestamp() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		Instant eventTime = LocalDate.of(2025, 2, 25).atStartOfDay(ZoneOffset.UTC).toInstant();
		Event event = new Event(new Bid(1L, 1L, 10L, "ch", "url", eventTime, ""));
		assertEquals(EPOCH_2025_02_25, gen.computePartitionValueMs(event));
	}

	// -------------------------------------------------------------------------
	// LATEST mode
	// -------------------------------------------------------------------------

	@Test
	public void testLatestModeAlwaysReturnsFirstPartition() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.LATEST;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		assertEquals(EPOCH_2025_02_27, gen.computePartitionValueMs(personEvent(1L, Instant.EPOCH)));
		assertEquals(EPOCH_2025_02_27, gen.computePartitionValueMs(personEvent(999L, Instant.EPOCH)));
	}

	// -------------------------------------------------------------------------
	// UNIFORM mode
	// -------------------------------------------------------------------------

	@Test
	public void testUniformModeIsDeterministic() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.UNIFORM;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		Event event = personEvent(42L, Instant.EPOCH);
		long first = gen.computePartitionValueMs(event);
		for (int i = 0; i < 10; i++) {
			assertEquals(first, gen.computePartitionValueMs(event));
		}
	}

	@Test
	public void testUniformModeSpreadsAcrossAllPartitions() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.UNIFORM;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		Set<Long> seen = new HashSet<>();
		for (int i = 0; i < 30; i++) {
			seen.add(gen.computePartitionValueMs(personEvent(i, Instant.EPOCH)));
		}
		// With 30 events and 3 partitions (hash-based), all 3 should be hit
		assertEquals(3, seen.size());
	}

	// -------------------------------------------------------------------------
	// RANDOM mode
	// -------------------------------------------------------------------------

	@Test
	public void testRandomModeOutputIsAlwaysAKnownPartition() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.RANDOM;
		conf.partitionValues = "2025-02-27,2025-02-26";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		Event event = personEvent(1L, Instant.EPOCH);
		for (int i = 0; i < 20; i++) {
			long v = gen.computePartitionValueMs(event);
			assertTrue(v == EPOCH_2025_02_27 || v == EPOCH_2025_02_26);
		}
	}

	// -------------------------------------------------------------------------
	// CUSTOM mode — value=weight format
	// -------------------------------------------------------------------------

	@Test
	public void testCustomModeValueEqualsWeightOnlyUsesConfiguredValues() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.CUSTOM;
		conf.partitionDistribution = "2025-02-27=80,2025-02-26=15,2025-02-25=5";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		long v = gen.computePartitionValueMs(personEvent(1L, Instant.EPOCH));
		assertTrue(v == EPOCH_2025_02_27 || v == EPOCH_2025_02_26 || v == EPOCH_2025_02_25);
	}

	@Test
	public void testCustomModeValueEqualsWeightHeavilyFavorsFirstBucket() {
		// weight 99:1 → almost all events go to first partition
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.CUSTOM;
		conf.partitionDistribution = "2025-02-27=99,2025-02-26=1";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		int firstCount = 0;
		for (int i = 0; i < 100; i++) {
			if (gen.computePartitionValueMs(personEvent(i, Instant.EPOCH)) == EPOCH_2025_02_27) firstCount++;
		}
		assertTrue("Expected majority in first partition, got " + firstCount, firstCount >= 90);
	}

	// -------------------------------------------------------------------------
	// CUSTOM mode — weights-only format
	// -------------------------------------------------------------------------

	@Test
	public void testCustomModeWeightsOnlyOnlyUsesConfiguredValues() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.CUSTOM;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		conf.partitionDistribution = "80,15,5";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		long v = gen.computePartitionValueMs(personEvent(1L, Instant.EPOCH));
		assertTrue(v == EPOCH_2025_02_27 || v == EPOCH_2025_02_26 || v == EPOCH_2025_02_25);
	}

	// -------------------------------------------------------------------------
	// SKEWED mode — Zipfian
	// -------------------------------------------------------------------------

	@Test
	public void testSkewedZipfianOnlyUsesConfiguredValues() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.SKEWED;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		conf.partitionDistribution = "zipfian";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		long v = gen.computePartitionValueMs(personEvent(1L, Instant.EPOCH));
		assertTrue(v == EPOCH_2025_02_27 || v == EPOCH_2025_02_26 || v == EPOCH_2025_02_25);
	}

	@Test
	public void testSkewedZipfianWithExponentOnlyUsesConfiguredValues() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.SKEWED;
		conf.partitionValues = "2025-02-27,2025-02-26";
		conf.partitionDistribution = "zipfian:1.5";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		long v = gen.computePartitionValueMs(personEvent(1L, Instant.EPOCH));
		assertTrue(v == EPOCH_2025_02_27 || v == EPOCH_2025_02_26);
	}

	@Test
	public void testSkewedZipfianFavorsFirstPartition() {
		// Zipfian strongly concentrates on the first partition
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.SKEWED;
		conf.partitionValues = "2025-02-27,2025-02-26,2025-02-25";
		conf.partitionDistribution = "zipfian:2.0";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		int firstCount = 0;
		for (int i = 0; i < 100; i++) {
			if (gen.computePartitionValueMs(personEvent(i, Instant.EPOCH)) == EPOCH_2025_02_27) firstCount++;
		}
		assertTrue("Zipfian should favor first partition, got " + firstCount, firstCount > 50);
	}

	// -------------------------------------------------------------------------
	// SKEWED with numeric weights treated as CUSTOM (backward compatibility)
	// -------------------------------------------------------------------------

	@Test
	public void testSkewedWithNumericWeightsTreatedAsCustom() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.SKEWED;
		conf.partitionValues = "2025-02-27,2025-02-26";
		conf.partitionDistribution = "70,30";
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		long v = gen.computePartitionValueMs(personEvent(1L, Instant.EPOCH));
		assertTrue(v == EPOCH_2025_02_27 || v == EPOCH_2025_02_26);
	}

	// -------------------------------------------------------------------------
	// partitionNumber generates date range
	// -------------------------------------------------------------------------

	@Test
	public void testPartitionNumberGeneratesDateRange() {
		NexmarkConfiguration conf = new NexmarkConfiguration();
		conf.partitionKeyField = "timestamp";
		conf.partitionDistributionMode = PartitionDistributionMode.LATEST;
		conf.partitionNumber = 3;
		PartitionValueGenerator gen = new PartitionValueGenerator(configWith(conf));

		// LATEST with auto-generated dates → always returns today (first in list)
		LocalDate today = LocalDate.now(ZoneOffset.UTC);
		long expectedToday = today.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
		assertEquals(expectedToday, gen.computePartitionValueMs(personEvent(1L, Instant.EPOCH)));
	}
}
