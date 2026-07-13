# Workload Synthesizer

A tool inside lake-loader that walks an existing Hudi table's timeline and emits a lake-loader configuration that reproduces the observed workload characteristics. Ships in the same uber-jar as `ChangeDataGenerator` and `IncrementalLoader`.

## Motivation

Benchmarking ingestion for open table formats — Hudi, Iceberg, Delta — is only useful when the benchmark **shape** resembles the production workload the customer actually cares about. The two failure modes are equally bad:

1. **Benchmark too synthetic.** We run lake-loader with defaults (1M records per round, uniform partition distribution, 50/50 insert/update), publish a number, and the customer says "that doesn't look anything like what my table does." True — their production is 8 partitions receiving 90% of updates, a heavy Zipfian skew on inserts, bursty daily commit sizes, and a UUID key that pushes every row into a different file group.
2. **Benchmark not portable.** We ask the customer to hand us a sample of their data so we can measure "what your workload looks like." That immediately runs into data-sharing constraints — legal review, PII, sample size, storage location. A three-week delay while the sample gets sanitized, and by then the benchmarking window has moved.

The workload synthesizer collapses this to a low-friction handoff. The customer runs one Spark job **inside their own environment** against a Hudi table already sitting in their lake, and gets back three text files. Two are shell-ready flag strings for lake-loader; one is a human-readable audit. They send us those three files plus their Avro schema (`.avsc`), and we run the benchmark on our side in EMR/EKS against the same shape of workload — no raw data ever crosses the perimeter.

## What it does

For each completed commit in the table's active timeline (optionally including archived), the synthesizer:

1. Deserializes `HoodieCommitMetadata` from the timeline via `metaClient.getTimelineLayout.getCommitMetadataSerDe`.
2. Walks the per-partition list of `HoodieWriteStat`, extracting `numInserts`, `numUpdateWrites`, `totalWriteBytes`, `fileSizeInBytes`, and `prevCommit` per file group.
3. Aggregates those numbers across commits into a `DerivedConfig` (records/round, update ratio, per-partition insert share, per-commit zipf shape fit, compressed bytes/record, fresh-file median size).
4. Reads `hoodie.properties` for the record-key column and key generator, then samples a few hundred values from a base parquet file to classify the key shape (UUID-like → `Random`; monotonic epoch prefix → `TemporallyOrdered`; ambiguous → `Random` with a warning in the audit).
5. Emits three files:
   - `synth-full.flags` — per-commit fidelity. One `--number-records-per-round` entry per source commit, preserving temporal variation.
   - `synth-summary.flags` — median records/round collapsed into a single value, for quick sanity runs.
   - `synth-audit.txt` — raw derived numbers, fitted zipf shapes per commit, key-classification reasoning.

The customer only needs to fill in `--path` (their benchmark output location) and `--avro-schema` (their schema file). Everything else is already there.

## What each derived value tells the benchmark

| Emitted flag | What it captures | How it's derived |
|---|---|---|
| `--number-rounds` | Total commit count over the analyzed window | `count(completed write commits after --since-instant, capped by --max-commits)` |
| `--number-records-per-round` | Per-commit total write volume — captures bursty daily/weekly patterns | List of `sum(numInserts + numUpdateWrites)` per commit, comma-joined |
| `--total-partitions` | Fan-out of the source table | Distinct partition paths ever touched |
| `--update-ratio` | How much of a batch is updates vs inserts | Mean of `updates / (inserts + updates)` across commits with any writes |
| `--num-partitions-to-update` | How many partitions get modified per batch | Median of "partitions with any update" per commit |
| `--record-size` | Compressed bytes/record — drives file count and shuffle cost | `sum(totalWriteBytes) / sum(numWrites)` across all commits |
| `--datagen-file-size` | Target parquet file size | Median `fileSizeInBytes` for stats where `prevCommit == null` (freshly-created base files) |
| `--update-pattern` + `--zipfian-shape` | Whether updates are uniform or skewed, and how skewed | OLS fit of `log(count) vs log(rank)` per commit, then median across commits; threshold `--min-zipf-shape` (default 0.3) decides Uniform vs Zipf |
| `--partition-distribution` | Where new inserts go | Aggregated per-partition insert share, sorted desc, normalized. Two-segment `first;subsequent` form used if round-0 shape materially differs from later rounds |
| `--primary-key-type` | Random UUIDs vs temporally-ordered keys — determines file-group fanout and index cost | Read record-key field from `hoodie.properties`, sample 500 values from a base parquet, classify by regex against UUID / epoch-prefix / monotonic patterns |

## Why this matters for open-table-format benchmarking

Each of those values controls a distinct axis of ingestion cost, and getting them wrong changes the answer:

- **Update ratio + partition skew** together determine index-lookup pressure. A Hudi RLI benchmark with 5% updates uniformly spread across 365 partitions looks fundamentally different from 40% updates concentrated in 8 partitions. Same total-record throughput, wildly different index cost.
- **Zipfian shape** on inserts drives file-group amplification. High-skew workloads create hot partitions that produce many small files unless bucket sizing is tuned; the benchmark needs to see the same skew to expose that.
- **Records per round** as a *distribution* — not just a mean — matters because bursty commits pressure the checkpoint SLA differently than steady-state commits. The `synth-full.flags` output preserves the actual per-commit sequence.
- **Key type** (Random vs TemporallyOrdered) changes update locality. UUIDs touch every file group; timestamp-prefixed keys concentrate hits on the newest ones. Bloom filter false-positive rates, compaction fan-out, and clustering benefit all depend on this.
- **Record size** derived from measured compressed bytes/record — not from the schema alone — makes parallelism estimates realistic. A schema with `binary` fields can inflate 5× under compression depending on payload entropy.
- **Fresh-file median size** picks a target file size that matches the source table's actual write pattern, so the benchmark doesn't inadvertently exercise a different compaction regime.

The point is not that these values are exotic — most of them are things a careful engineer would set by hand. The point is that they are **derived from the real table** rather than guessed, and the whole pipeline runs unattended in the customer's environment with a single `spark-submit`.

## Handoff flow

```
   ┌──────────────────────────┐        ┌──────────────────────────────────────┐
   │  Customer environment    │        │  Onehouse benchmark env              │
   │  (production data plane) │        │  (EMR / EKS, isolated)               │
   ├──────────────────────────┤        ├──────────────────────────────────────┤
   │                          │        │                                      │
   │  Hudi table              │        │  spark-submit                        │
   │  (large, sensitive)      │        │    --class WorkloadResizer           │
   │        │                 │        │    ... --scale-factor 0.01           │
   │        ▼                 │        │           --target-partitions 300    │
   │  spark-submit            │        │        │                             │
   │    --class               │        │        ▼                             │
   │      WorkloadSynthesizer │        │  resized-full.flags                  │
   │    lake-loader.jar       │        │        │                             │
   │        │                 │        │        ▼                             │
   │        ▼                 │        │  spark-submit                        │
   │  synth-full.flags        │        │    --class ChangeDataGenerator       │
   │  synth-summary.flags     │        │    @resized-full.flags               │
   │  synth-audit.txt         │──────▶ │        │                             │
   │  synth-derived.json      │  send  │        ▼                             │
   │  schema.avsc (anon.)     │        │  Synthetic workload matching         │
   └──────────────────────────┘        │  benchmark-sized production shape    │
                                        │        │                             │
                                        │        ▼                             │
                                        │  Ingestion benchmark                 │
                                        │  (Hudi / Iceberg / Delta)            │
                                        └──────────────────────────────────────┘
```

The four text files plus the (optionally anonymized) Avro schema are the entire artifact set. They are small (kilobytes), reviewable, and contain no row-level data.

## Two-stage pipeline: Synthesizer + Resizer

Real customer tables are often too big to benchmark at 1:1 scale — a 1 PB / 3000-partition table can't be exercised in a reasonable-sized EMR/EKS test environment. So the pipeline is split in two:

1. **WorkloadSynthesizer** (customer side): observes a real Hudi table, captures its shape as a set of derived parameters, and emits both a human-readable flag file (`synth-full.flags`) and a machine-readable ground truth (`synth-derived.json`).
2. **WorkloadResizer** (benchmarking side): consumes `synth-derived.json`, applies a scale factor and/or a target partition count, and emits a scaled configuration (`resized-full.flags`) suitable for the target benchmark environment.

The split matters because scaling is inherently *destination-driven*: the customer knows their production shape but doesn't know what our benchmark cluster can handle. Letting each side own its half keeps concerns clean and lets us iterate on benchmark sizing without re-running the (Spark-based) synthesizer against the customer table.

**What the Resizer scales, and what it preserves.** Two independent knobs:
- `--scale-factor F` multiplies each per-round record count by F. `numRounds` is unchanged, so the workload's temporal cadence (bursty commits stay bursty, quiet commits stay quiet) is preserved.
- `--target-partitions M` overrides `totalPartitions`. When M < source, the partition-distribution vector is truncated and re-normalized. When M > source, the fitted zipf shape is *extrapolated* to a length-M weight vector via `p(rank) ∝ 1/rank^s`. `numPartitionsToUpdate` is rescaled to preserve the same *fraction* of updated partitions as the source (e.g. 21/3000 → ceil(0.7% × 300) = 3).

**Invariants under resizing** — never touched: `updateRatio`, `updatePattern`, `zipfianShape`, `primary-key-type`, `recordSize`, `targetDataFileSize`, `schemaChoice`. These are per-record or per-workload-character properties; scaling data volume shouldn't change them.

Example workflow:

```bash
# On benchmarking side, downscale a 3000-partition 1 PB table to 300 partitions × 1% volume
spark-submit --class ai.onehouse.lakeloader.WorkloadResizer lake-loader-0.2.jar \
  --input-json  /path/to/synth-derived.json \
  --output-dir  /path/to/resized-configs \
  --scale-factor    0.01 \
  --target-partitions 300

# Then run the benchmark
spark-submit --class ai.onehouse.lakeloader.ChangeDataGenerator lake-loader-0.2.jar \
  @/path/to/resized-configs/resized-full.flags
```

The Resizer also emits a `resized-audit.txt` with before/after values for every changed parameter and an explicit list of the preserved invariants, so reviewers can verify the scaling was applied as intended.

## Command-line surface

```
spark-submit --class ai.onehouse.lakeloader.WorkloadSynthesizer lake-loader-0.2.jar \
  --table-path <hudi-table-location> \
  --output-dir <local-or-hadoop-fs-path> \
  [--max-commits <n>] \
  [--since-instant <instant>] \
  [--include-archived true] \
  [--min-zipf-shape 0.3] \
  [--primary-key-type Random|TemporallyOrdered] \
  [--key-sample-size 500]
```

`--max-commits` and `--since-instant` bound the analysis window when a table has a very long history — usually we want the last few weeks, not multi-year archive. `--primary-key-type` skips inference if the customer already knows the shape of their keys. `--min-zipf-shape` is the cutoff below which the fitted skew is treated as uniform (i.e. don't emit `Zipf` for statistical noise).

## Schema handling

Three cases, chosen by the `--schema-file` and `--anonymize-schema` flags:

1. **Customer supplies `.avsc`, no anonymization.** Emitted flags reference the customer's original schema path via `--avro-schema`. Best fidelity; requires the customer to share column names.
2. **Customer supplies `.avsc`, `--anonymize-schema true`.** Field names are rewritten to typed placeholders (`col_long_a`, `col_string_b`, …); the anonymized schema is written to `outputDir/schema.avsc`, and the emitted flags reference that. Data types, nullability, and nested record structure are preserved.
3. **No `.avsc` supplied.** Tool reads the source Hudi table's schema, counts top-level fields, and emits `--number-columns <n>` instead of `--avro-schema`. Lake-loader's generator falls back to its default flat schema with the same column arity. No schema file is written unless `--anonymize-schema true`, in which case the inferred schema is anonymized and written to `outputDir/schema.avsc`.

The typical customer flow is case 2 (or case 3 with anonymization on): the tool reads the source table's schema, strips names, and ships an `.avsc` matching the exact type layout. Data types alone drive parquet page layout, compression, and column-store cost — the names are not benchmark-relevant, and stripping them removes the last data-sharing concern.

## What the tool does *not* do

- It does not read row values except a small key-column sample for classification, and even that only reads the record-key column of one parquet file.
- It does not attempt to model schema evolution across commits. Only the latest schema (from `hoodie.properties` / table create schema) is captured.
- It does not model composite record keys — those fall back to `Random` with a warning in the audit.
- It does not attempt to reproduce delete workloads. Deletes are noted in the audit but not emitted as flags; lake-loader's data generator doesn't model them today.

## Verification path

1. Run against a known-shape synthetic Hudi table (10 partitions, controlled zipf skew, known update ratio). Assert the emitted flags round-trip through `ChangeDataGeneratorParser` cleanly, and that the derived values are within tolerance of the ground truth.
2. Feed the emitted `synth-full.flags` into `ChangeDataGenerator`, generate the synthetic workload, and cross-check the *output* table's per-partition write counts against the *source* table's via the `hudi-commit-stats` skill. A tight match (within ~5% on partition share, ~0.1 on fitted zipf shape) confirms the loop closes.
3. Confirm the synthesizer's `recordSize` and `ChangeDataGenerator.estimateRecordSize` (the sample-write path for custom Avro schemas) land within ~2× of each other — they should, both measure compressed parquet bytes/record.
