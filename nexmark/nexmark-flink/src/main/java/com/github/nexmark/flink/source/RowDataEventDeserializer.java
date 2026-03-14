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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.model.Person;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RowDataEventDeserializer implements EventDeserializer<RowData> {

	private final GeneratorConfig config;
	private final boolean hasPartitionColumn;
	private final PartitionDistributionMode effectiveMode;
	private final List<String> partitionValuesList;
	private final int[] cumulativeWeights;
	/** Zipfian cumulative distribution (normalized, last element 1.0); used when non-null for SKEWED. */
	private final double[] cumulativeZipfian;

	public RowDataEventDeserializer() {
		this(null);
	}

	public RowDataEventDeserializer(GeneratorConfig config) {
		this.config = config;
		this.hasPartitionColumn = config != null && config.getPartitionKeyField() != null && !config.getPartitionKeyField().isEmpty();

		PartitionDistributionMode mode = config != null ? config.getPartitionDistributionMode() : PartitionDistributionMode.UNIFORM;
		int partitionNumber = config != null ? config.getPartitionNumber() : 0;
		List<String> dateListFromNumber = partitionNumber > 0 ? buildPartitionDatesFromToday(partitionNumber) : Collections.emptyList();
		List<String> valuesFromSpec = parsePartitionValues(config != null ? config.getPartitionValues() : null);
		List<String> valuesToUse = !dateListFromNumber.isEmpty() ? dateListFromNumber : valuesFromSpec;

		List<String> skewedValues = new ArrayList<>();
		List<Integer> skewedWeights = new ArrayList<>();
		boolean hasSkewedSpec = hasPartitionColumn && config != null && config.getPartitionDistribution() != null && !config.getPartitionDistribution().isEmpty();
		String distSpec = hasSkewedSpec ? config.getPartitionDistribution().trim() : "";
		boolean skewedZipfian = hasSkewedSpec && distSpec.toLowerCase().startsWith("zipfian") && mode == PartitionDistributionMode.SKEWED && !valuesToUse.isEmpty();
		double[] zipfianCumulative = null;
		if (skewedZipfian) {
			double exponent = parseZipfianExponent(distSpec);
			zipfianCumulative = buildZipfianCumulative(valuesToUse.size(), exponent);
		}
		boolean skewedUsesValueEquals = hasSkewedSpec && !skewedZipfian && distSpec.contains("=");
		if (hasSkewedSpec && !skewedZipfian) {
			if (skewedUsesValueEquals) {
				parsePartitionDistribution(config.getPartitionDistribution(), skewedValues, skewedWeights);
			} else if ((mode == PartitionDistributionMode.SKEWED || mode == PartitionDistributionMode.CUSTOM) && !valuesToUse.isEmpty()) {
				parseWeightsOnly(config.getPartitionDistribution(), valuesToUse.size(), skewedWeights);
				skewedValues.addAll(valuesToUse);
			}
		}

		if (skewedZipfian && zipfianCumulative != null) {
			this.effectiveMode = PartitionDistributionMode.SKEWED;
			this.partitionValuesList = valuesToUse;
			this.cumulativeWeights = null;
			this.cumulativeZipfian = zipfianCumulative;
		} else if (hasSkewedSpec && skewedUsesValueEquals && !skewedValues.isEmpty()) {
			this.effectiveMode = PartitionDistributionMode.CUSTOM;
			this.partitionValuesList = skewedValues;
			this.cumulativeWeights = buildCumulativeWeights(skewedWeights);
			this.cumulativeZipfian = null;
		} else if (hasSkewedSpec && !skewedUsesValueEquals && !skewedValues.isEmpty() && !skewedWeights.isEmpty()) {
			int sum = skewedWeights.stream().mapToInt(Integer::intValue).sum();
			if (sum == 0) {
				for (int i = 0; i < skewedWeights.size(); i++) skewedWeights.set(i, 1);
			}
			this.effectiveMode = PartitionDistributionMode.CUSTOM;
			this.partitionValuesList = skewedValues;
			this.cumulativeWeights = buildCumulativeWeights(skewedWeights);
			this.cumulativeZipfian = null;
		} else if (mode == PartitionDistributionMode.LATEST && !valuesToUse.isEmpty()) {
			this.effectiveMode = PartitionDistributionMode.LATEST;
			this.partitionValuesList = valuesToUse;
			this.cumulativeWeights = null;
			this.cumulativeZipfian = null;
		} else if ((mode == PartitionDistributionMode.UNIFORM || mode == PartitionDistributionMode.RANDOM) && !valuesToUse.isEmpty()) {
			this.effectiveMode = mode;
			this.partitionValuesList = valuesToUse;
			this.cumulativeWeights = null;
			this.cumulativeZipfian = null;
		} else {
			this.effectiveMode = null;
			this.partitionValuesList = null;
			this.cumulativeWeights = null;
			this.cumulativeZipfian = null;
		}
	}

	/** Parse "zipfian" or "zipfian:1.5" -> exponent (default 1.0). */
	private static double parseZipfianExponent(String spec) {
		if (spec == null) return 1.0;
		String s = spec.trim().toLowerCase();
		if (!s.startsWith("zipfian")) return 1.0;
		int colon = s.indexOf(':');
		if (colon < 0) return 1.0;
		try {
			double e = Double.parseDouble(s.substring(colon + 1).trim());
			return e > 0 ? e : 1.0;
		} catch (NumberFormatException ignored) {
			return 1.0;
		}
	}

	/** Build cumulative Zipfian distribution: weight_i = 1/(i+1)^exponent, normalized so last = 1.0. */
	private static double[] buildZipfianCumulative(int n, double exponent) {
		if (n <= 0) return null;
		double[] weights = new double[n];
		double sum = 0;
		for (int i = 0; i < n; i++) {
			weights[i] = 1.0 / Math.pow(i + 1, exponent);
			sum += weights[i];
		}
		double[] cum = new double[n];
		double acc = 0;
		for (int i = 0; i < n; i++) {
			acc += weights[i] / sum;
			cum[i] = acc;
		}
		cum[n - 1] = 1.0;
		return cum;
	}

	/** Build [today, today-1, ..., today-(partitionNumber-1)] in yyyy-MM-dd (UTC). */
	private static List<String> buildPartitionDatesFromToday(int partitionNumber) {
		if (partitionNumber <= 0) return Collections.emptyList();
		LocalDate today = LocalDate.now(ZoneOffset.UTC);
		List<String> list = new ArrayList<>(partitionNumber);
		for (int i = 0; i < partitionNumber; i++) {
			list.add(today.minusDays(i).format(DateTimeFormatter.ISO_LOCAL_DATE));
		}
		return list;
	}

	/** Parse comma-separated weights only (no value=weight), length expected = partition count; pad or truncate. */
	private static void parseWeightsOnly(String spec, int expectedCount, List<Integer> weights) {
		if (spec == null || spec.isEmpty()) return;
		for (String s : spec.split(",")) {
			try {
				weights.add(Integer.parseInt(s.trim()));
			} catch (NumberFormatException ignored) {
			}
		}
		while (weights.size() < expectedCount) {
			weights.add(0);
		}
		if (weights.size() > expectedCount) {
			weights.subList(expectedCount, weights.size()).clear();
		}
	}

	private static List<String> parsePartitionValues(String spec) {
		if (spec == null || spec.isEmpty()) return Collections.emptyList();
		return Arrays.stream(spec.split(","))
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(Collectors.toList());
	}

	private static void parsePartitionDistribution(String spec, List<String> values, List<Integer> weights) {
		int total = 0;
		for (String pair : spec.split(",")) {
			String trimmed = pair.trim();
			int eq = trimmed.indexOf('=');
			if (eq <= 0) continue;
			String value = trimmed.substring(0, eq).trim();
			String wStr = trimmed.substring(eq + 1).trim();
			try {
				int w = Integer.parseInt(wStr);
				if (w > 0 && !value.isEmpty()) {
					values.add(value);
					weights.add(w);
					total += w;
				}
			} catch (NumberFormatException ignored) {
			}
		}
		if (total == 0 && !values.isEmpty()) {
			// normalize to sum 100
			for (int i = 0; i < weights.size(); i++) {
				weights.set(i, 100 / values.size());
			}
		}
	}

	private static int[] buildCumulativeWeights(List<Integer> weights) {
		if (weights == null || weights.isEmpty()) return null;
		int sum = 0;
		for (int w : weights) sum += w;
		int[] cum = new int[weights.size()];
		int acc = 0;
		for (int i = 0; i < weights.size(); i++) {
			acc += weights.get(i);
			cum[i] = acc;
		}
		return cum;
	}

	@Override
	public RowData deserialize(Event event) {
		return convertEvent(event);
	}

	private RowData convertEvent(Event event) {
		int numFields = hasPartitionColumn ? 5 : 4;
		GenericRowData rowData = new GenericRowData(numFields);
		rowData.setField(0, event.type.value);
		if (event.type == Event.Type.PERSON) {
			assert event.newPerson != null;
			rowData.setField(1, convertPerson(event.newPerson));
		} else if (event.type == Event.Type.AUCTION) {
			assert event.newAuction != null;
			rowData.setField(2, convertAuction(event.newAuction));
		} else if (event.type == Event.Type.BID) {
			assert event.bid != null;
			rowData.setField(3, convertBid(event.bid));
		} else {
			throw new UnsupportedOperationException("Unsupported event type: " + event.type.name());
		}
		if (hasPartitionColumn) {
			long partitionValueMs = computePartitionValueMs(event);
			rowData.setField(4, partitionValueMs);
		}
		return rowData;
	}

	/** Returns partition value as epoch milliseconds (start of day UTC for date-based partitions). */
	private long computePartitionValueMs(Event event) {
		String dateStr = null;
		if (effectiveMode == PartitionDistributionMode.SKEWED && partitionValuesList != null && cumulativeZipfian != null && cumulativeZipfian.length > 0) {
			int hash = eventHash(event);
			double u = (hash & 0x7FFFFFFFL) / (Integer.MAX_VALUE + 1.0);
			int idx = 0;
			for (int i = 0; i < cumulativeZipfian.length; i++) {
				if (u < cumulativeZipfian[i]) {
					idx = i;
					break;
				}
				idx = i;
			}
			dateStr = partitionValuesList.get(Math.min(idx, partitionValuesList.size() - 1));
		} else if (effectiveMode == PartitionDistributionMode.CUSTOM && partitionValuesList != null && cumulativeWeights != null && cumulativeWeights.length > 0) {
			int hash = eventHash(event);
			int total = cumulativeWeights[cumulativeWeights.length - 1];
			int bucket = Math.floorMod(hash, total);
			for (int i = 0; i < cumulativeWeights.length; i++) {
				if (bucket < cumulativeWeights[i]) {
					dateStr = partitionValuesList.get(i);
					break;
				}
			}
			if (dateStr == null) dateStr = partitionValuesList.get(partitionValuesList.size() - 1);
		} else if (effectiveMode == PartitionDistributionMode.LATEST && partitionValuesList != null && !partitionValuesList.isEmpty()) {
			dateStr = partitionValuesList.get(0);
		} else if ((effectiveMode == PartitionDistributionMode.UNIFORM || effectiveMode == PartitionDistributionMode.RANDOM) && partitionValuesList != null && !partitionValuesList.isEmpty()) {
			int hash = eventHash(event);
			int idx = Math.floorMod(hash, partitionValuesList.size());
			dateStr = partitionValuesList.get(idx);
		}
		if (dateStr != null) {
			return dateStringToEpochMilli(dateStr);
		}
		// no mode or fallback: derive from event timestamp (start of day UTC)
		Instant instant = getEventTimestamp(event);
		LocalDate day = instant.atZone(ZoneOffset.UTC).toLocalDate();
		return day.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
	}

	/** Parse yyyy-MM-dd to start-of-day UTC epoch milliseconds. */
	private static long dateStringToEpochMilli(String dateStr) {
		try {
			LocalDate d = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
			return d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
		} catch (DateTimeParseException e) {
			return 0L;
		}
	}

	private static Instant getEventTimestamp(Event event) {
		if (event.newPerson != null) return event.newPerson.dateTime;
		if (event.newAuction != null) return event.newAuction.dateTime;
		if (event.bid != null) return event.bid.dateTime;
		return Instant.EPOCH;
	}

	private static int eventHash(Event event) {
		int h = event.type.value;
		if (event.newPerson != null) {
			h = 31 * h + Long.hashCode(event.newPerson.id);
		} else if (event.newAuction != null) {
			h = 31 * h + Long.hashCode(event.newAuction.id);
		} else if (event.bid != null) {
			h = 31 * h + Long.hashCode(event.bid.auction);
			h = 31 * h + Long.hashCode(event.bid.bidder);
			h = 31 * h + Long.hashCode(event.bid.price);
		}
		return h;
	}

	private RowData convertPerson(Person person) {
		GenericRowData rowData = new GenericRowData(8);
		rowData.setField(0, person.id);
		rowData.setField(1, StringData.fromString(person.name));
		rowData.setField(2, StringData.fromString(person.emailAddress));
		rowData.setField(3, StringData.fromString(person.creditCard));
		rowData.setField(4, StringData.fromString(person.city));
		rowData.setField(5, StringData.fromString(person.state));
		rowData.setField(6, TimestampData.fromInstant(person.dateTime));
		rowData.setField(7, StringData.fromString(person.extra));
		return rowData;
	}

	private RowData convertAuction(Auction auction) {
		GenericRowData rowData = new GenericRowData(10);
		rowData.setField(0, auction.id);
		rowData.setField(1, StringData.fromString(auction.itemName));
		rowData.setField(2, StringData.fromString(auction.description));
		rowData.setField(3, auction.initialBid);
		rowData.setField(4, auction.reserve);
		rowData.setField(5, TimestampData.fromInstant(auction.dateTime));
		rowData.setField(6, TimestampData.fromInstant(auction.expires));
		rowData.setField(7, auction.seller);
		rowData.setField(8, auction.category);
		rowData.setField(9, StringData.fromString(auction.extra));
		return rowData;
	}

	private RowData convertBid(Bid bid) {
		GenericRowData rowData = new GenericRowData(7);
		rowData.setField(0, bid.auction);
		rowData.setField(1, bid.bidder);
		rowData.setField(2, bid.price);
		rowData.setField(3, StringData.fromString(bid.channel));
		rowData.setField(4, StringData.fromString(bid.url));
		rowData.setField(5, TimestampData.fromInstant(bid.dateTime));
		rowData.setField(6, StringData.fromString(bid.extra));
		return rowData;
	}
}
