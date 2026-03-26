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

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.model.Event;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Computes partition values for Nexmark events based on a configured distribution mode.
 *
 * <p>Supports five modes: UNIFORM (deterministic hash-based), LATEST (always the most recent
 * partition), RANDOM (non-deterministic), SKEWED (Zipfian power-law), and CUSTOM (explicit
 * per-value weights). When no mode is active, falls back to deriving the partition from the
 * event's own timestamp (start of day, UTC).
 */
public class PartitionValueGenerator {

	private final PartitionDistributionMode effectiveMode;
	private final List<String> partitionValuesList;
	private final int[] cumulativeWeights;
	private final double[] cumulativeZipfian;

	public PartitionValueGenerator(GeneratorConfig config) {
		PartitionDistributionMode mode = config != null ? config.getPartitionDistributionMode() : PartitionDistributionMode.UNIFORM;
		int partitionNumber = config != null ? config.getPartitionNumber() : 0;
		List<String> dateListFromNumber = partitionNumber > 0 ? buildPartitionDatesFromToday(partitionNumber) : Collections.emptyList();
		List<String> valuesFromSpec = parsePartitionValues(config != null ? config.getPartitionValues() : null);
		List<String> valuesToUse = !dateListFromNumber.isEmpty() ? dateListFromNumber : valuesFromSpec;

		boolean hasPartitionColumn = config != null && config.getPartitionKeyField() != null && !config.getPartitionKeyField().isEmpty();
		List<String> skewedValues = new ArrayList<>();
		List<Integer> skewedWeights = new ArrayList<>();
		boolean hasSkewedSpec = hasPartitionColumn && config.getPartitionDistribution() != null && !config.getPartitionDistribution().isEmpty();
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

	public long computePartitionValueMs(Event event) {
		if (effectiveMode == null) {
			return startOfDayUtcMs(getEventTimestamp(event).toEpochMilli());
		}
		int hash = event.hashCode();
		switch (effectiveMode) {
			case SKEWED: {
				double u = (hash & 0x7FFFFFFFL) / (Integer.MAX_VALUE + 1.0);
				int idx = cumulativeZipfian.length - 1;
				for (int i = 0; i < cumulativeZipfian.length; i++) {
					if (u < cumulativeZipfian[i]) { idx = i; break; }
				}
				return dateStringToEpochMilli(partitionValuesList.get(idx));
			}
			case CUSTOM: {
				int bucket = Math.floorMod(hash, cumulativeWeights[cumulativeWeights.length - 1]);
				for (int i = 0; i < cumulativeWeights.length; i++) {
					if (bucket < cumulativeWeights[i]) return dateStringToEpochMilli(partitionValuesList.get(i));
				}
				return dateStringToEpochMilli(partitionValuesList.get(partitionValuesList.size() - 1));
			}
			case LATEST:
				return dateStringToEpochMilli(partitionValuesList.get(0));
			case UNIFORM:
				return dateStringToEpochMilli(partitionValuesList.get(Math.floorMod(hash, partitionValuesList.size())));
			case RANDOM:
				return dateStringToEpochMilli(partitionValuesList.get(ThreadLocalRandom.current().nextInt(partitionValuesList.size())));
			default:
				return startOfDayUtcMs(getEventTimestamp(event).toEpochMilli());
		}
	}

	/** Truncates an epoch-millisecond timestamp to the start of its UTC day. */
	private static long startOfDayUtcMs(long epochMs) {
		return epochMs - Math.floorMod(epochMs, 86_400_000L);
	}

	private static Instant getEventTimestamp(Event event) {
		if (event.newPerson != null) return event.newPerson.dateTime;
		if (event.newAuction != null) return event.newAuction.dateTime;
		if (event.bid != null) return event.bid.dateTime;
		return Instant.EPOCH;
	}

	private static double parseZipfianExponent(String spec) {
		String s = spec.trim().toLowerCase();
		int colon = s.indexOf(':');
		if (colon < 0) return 1.0;
		try {
			double e = Double.parseDouble(s.substring(colon + 1).trim());
			return e > 0 ? e : 1.0;
		} catch (NumberFormatException ignored) {
			return 1.0;
		}
	}

	/** weight_i = 1/(i+1)^exponent, normalized so cumulative[last] = 1.0 */
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

	private static List<String> buildPartitionDatesFromToday(int partitionNumber) {
		LocalDate today = LocalDate.now(ZoneOffset.UTC);
		List<String> list = new ArrayList<>(partitionNumber);
		for (int i = 0; i < partitionNumber; i++) {
			list.add(today.minusDays(i).format(DateTimeFormatter.ISO_LOCAL_DATE));
		}
		return list;
	}

	/** Parses comma-separated weights (no value=weight); pads with 0 or truncates to expectedCount. */
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
			for (int i = 0; i < weights.size(); i++) {
				weights.set(i, 100 / values.size());
			}
		}
	}

	private static int[] buildCumulativeWeights(List<Integer> weights) {
		if (weights == null || weights.isEmpty()) return null;
		int[] cum = new int[weights.size()];
		int acc = 0;
		for (int i = 0; i < weights.size(); i++) {
			acc += weights.get(i);
			cum[i] = acc;
		}
		return cum;
	}

	private static long dateStringToEpochMilli(String dateStr) {
		try {
			LocalDate d = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
			return d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
		} catch (DateTimeParseException e) {
			return 0L;
		}
	}
}
