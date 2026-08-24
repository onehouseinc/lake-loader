# Efficient Hudi bootstrap + ingestion benchmarking

How to benchmark incremental-ingestion latency on a large Hudi table without paying to write
that table repeatedly.

## Why

The straightforward lake-loader benchmark writes the dataset three times over. For a 10 TB
table:

1. datagen writes 10 TB of round-0 input, plus incremental batches;
2. the **baseline** run bulk-inserts all 10 TB into a Hudi table, then upserts each batch;
3. the **variant** run (Quanton, a different config, …) does the same 10 TB again.

At hundreds of TB this takes days and tends to OOM — and it is spent on a phase nobody measures.
Bootstrap latency is not the number we report; **incremental ingestion latency is**.

The efficient flow pays the real write cost exactly once, for a single partition, and
manufactures the rest of the table at the file level:

| | straightforward | efficient |
|---|---|---|
| Real Hudi writes during bootstrap | N partitions × full data | **1 partition** |
| Cost of adding partition #1000 | full write path | parquet page copy, few columns restamped |
| Cost per extra benchmark variant | another full bootstrap | copy the bootstrapped table |

The fan-out is done by `ParquetPartitionRewriteJob` (onehouse-dataplane,
[PR #2898](https://github.com/onehouseinc/onehouse-dataplane/pull/2898)). It copies a partition's
base files at the **page level without decoding them**, restamping only the partition columns,
the record-key suffix, `_hoodie_file_name`, and the file id. Cost scales with the rewritten
columns, not the file size.

## The four steps

```
 step 1              step 2                    step 3                step 4
┌──────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌───────────────────┐
│ generate │   │ fan out 1 → N    │   │ generate rounds  │   │ load rounds 1..N  │
│ + ingest │──▶│ partitions via   │──▶│ 1..N against the │──▶│ with RLI on;      │
│ 1 seed   │   │ rewrite job      │   │ bootstrapped tbl │   │ MEASURE THIS      │
│ (no MDT) │   │ (no Hudi write)  │   │                  │   │                   │
└──────────┘   └──────────────────┘   └──────────────────┘   └───────────────────┘
```

Only step 4 is timed. Steps 1–3 are setup, and their output can be **copied** to give each
benchmark variant an identical starting table.

Throughout, the loader runs on the **Spark datasource API** (`--api-type spark-datasource`, the
default — just omit the flag). The SQL API is not supported for this flow; see B5.

---

## Step 1 — seed one partition

Generate one partition of real data and ingest it with **the metadata table off**.

`seed_spec.json`:
```json
{
  "bootstrap": {
    "startDate": "2026-01-01",
    "endDate": "2026-01-01",
    "totalRecords": 10000,
    "suffixKeyWithPartitionPath": true
  },
  "commits": []
}
```

```bash
spark-submit --class ai.onehouse.lakeloader.ChangeDataGenerator target/lake-loader-0.2.jar \
  -p file:///tmp/llsanity/input \
  --workload-spec file:///tmp/llsanity/seed_spec.json \
  --record-size 1024

spark-submit --class ai.onehouse.lakeloader.IncrementalLoader target/lake-loader-0.2.jar \
  -i file:///tmp/llsanity/input \
  -o file:///tmp/llsanity/output \
  --experiment-id sanity_test \
  --format hudi --write-mode copy-on-write \
  --start-round 0 --number-rounds 1 \
  --initial-operation-type bulk_insert \
  --record-key-field key --partition-path-field partition \
  --initial-options \
      hoodie.metadata.enable=false,\
hoodie.datasource.write.hive_style_partitioning=false,\
hoodie.datasource.write.precombine.field=ts
```

`suffixKeyWithPartitionPath: true` makes seed keys `<uuid>-000_2026-01-01` — the normal
`--primary-key-type` key with `_<partition>` appended. **This is required.** The rewrite job
replaces the text after the *last* underscore; the default key scheme (`<uuid>-<round>`) has no
underscore, so the job would *append* a suffix instead of replacing one, and re-running the
fan-out would keep stacking suffixes.

The base key contains no underscore of its own, so the rewriter's split lands exactly on the
separator and the whole `<uuid>-<round>` prefix survives. Keeping the `%03d` round tag means a
key still names the round that created it even after being fanned out. Note the tag is no longer
the last 3 characters — extract it with `regexp_extract(key, "-(\\d{3})_", 1)`, not
`substring(key, -3, 3)`.

**Table path** is derived, not given: `genHudiTableName` builds
`<catalog>.<database>.hudi-<experimentId>` then replaces **every** `-` with `_`, and `getTablePath`
maps `.` to `/`. So the above yields:

```
file:///tmp/llsanity/output/spark_catalog/default/hudi_sanity_test
```

Use underscores in `--experiment-id`; hyphens get silently mangled. That path is `$TABLE` below.

**Assert:** partition dir is `2026-01-01`, *not* `partition=2026-01-01`; **no `.hoodie/metadata`
directory exists**; snapshot count 10000; keys match `^[0-9a-f-]{36}_2026-01-01$`.

---

## Step 2 — fan out to N partitions

One invocation per target partition (the job takes a single partition at a time).

```bash
TABLE=file:///tmp/llsanity/output/spark_catalog/default/hudi_sanity_test

for P in 2026-01-02 2026-01-03; do
  spark-submit --class com.onehouse.sparkjobs.parquet.ParquetPartitionRewriteJob \
    --jars parquet-rewriter-obfuscated.jar \
    dataplane-core.jar \
    --input-path      $TABLE/2026-01-01 \
    --output-path     $TABLE/$P \
    --table-base-path $TABLE \
    --partition-key   partition \
    --partition-value $P \
    --key-column      key
done
```

`--key-suffix-value` defaults to `--partition-value`, which is what we want. The rewriter jar must
be on the classpath alongside `dataplane-core.jar` (it is `compileOnlyApi` in that module).

No Hudi commit is written. The files keep the source write token and **instant time**, and that
instant is already a completed commit on the timeline — with `.hoodie_partition_metadata` present
(written by the job after all files land), the new partitions are picked up as committed file
slices. Preserving the instant time is load-bearing, not decorative; see B3 below.

**Assert:** three partition dirs, each containing `.hoodie_partition_metadata`; still no
`.hoodie/metadata`; the timeline is **unchanged** (same commit count as after step 1); snapshot
count 30000; each partition's keys end in that partition's own value; `_hoodie_partition_path`
agrees with the directory.

---

## Step 3 — generate incremental rounds

`batches_spec.json`:
```json
{
  "externalBootstrap": {
    "tablePath": "file:///tmp/llsanity/output/spark_catalog/default/hudi_sanity_test",
    "suffixKeyWithPartitionPath": true
  },
  "commits": [
    {"2026-01-01": {"inserts": 500, "updates": 1000},
     "2026-01-02": {"inserts": 500, "updates": 1000},
     "2026-01-03": {"inserts": 500, "updates": 1000}},
    {"2026-01-01": {"inserts": 500, "updates": 1000},
     "2026-01-02": {"inserts": 500, "updates": 1000},
     "2026-01-03": {"inserts": 500, "updates": 1000}}
  ]
}
```

```bash
spark-submit --class ai.onehouse.lakeloader.ChangeDataGenerator target/lake-loader-0.2.jar \
  -p file:///tmp/llsanity/input \
  --workload-spec file:///tmp/llsanity/batches_spec.json \
  --record-size 1024
```

Update targets are read back from the live table (`_hoodie_record_key` / `_hoodie_partition_path`,
partition- and column-pruned); inserts are freshly keyed as `<uuid>_<partition>`.

Note the two paths differ slightly: step-1 seed keys keep their round tag
(`<uuid>-000_<partition>`) while `externalBootstrap` inserts are a bare `<uuid>_<partition>`. Both
end in `_<partition>` and carry exactly one underscore, which is what the rewriter and any
suffix-based tooling care about; only the round tag is absent from the incremental inserts.

Round numbering starts at **1** — no round 0 is generated, which is the point.

**Assert:** `input/1` and `input/2` exist, `input/0` does not; 4500 records per round
(3 × 1500); update keys are a subset of the table's keys; insert keys are fresh and
partition-suffixed.

---

## Step 4 — load with RLI, and measure

Enable RLI here, for the first time. Run one round per invocation so each is timed separately:

```bash
for R in 1 2; do
  spark-submit --class ai.onehouse.lakeloader.IncrementalLoader target/lake-loader-0.2.jar \
    -i file:///tmp/llsanity/input \
    -o file:///tmp/llsanity/output \
    --experiment-id sanity_test \
    --format hudi --write-mode copy-on-write \
    --start-round $R --number-rounds 1 \
    --operation-type upsert \
    --record-key-field key --partition-path-field partition \
    --options \
        hoodie.metadata.enable=true,\
hoodie.metadata.record.index.enable=true,\
hoodie.index.type=RECORD_INDEX,\
hoodie.datasource.write.hive_style_partitioning=false,\
hoodie.datasource.write.precombine.field=ts
done
```

**How RLI picks up files Hudi never wrote.** When `record.index.enable` flips to `true` on a table
with no metadata table, Hudi runs `initializeFromFilesystem`, which does a recursive **storage
listing** from the base path to build the `files` partition, then builds `record_index` from it.
It never asks the timeline which files should exist. So the fanned-out files are indexed like any
others — provided step 1 left no metadata table behind (B3) and their instant time is `<=` the
latest completed instant (B4).

**Assert:** `.hoodie/metadata/record_index` exists and is non-empty; after round 1 the RLI entry
count is 31500 (30000 + 1500), the direct evidence that RLI bootstrapped over the fanned-out
partitions and not just the seed; after round 2 the snapshot count is **exactly 33000**.

That final number is the single most informative check. 33000 = 30000 + 2 rounds × 1500 inserts,
with updates landing in place. If updates were misrouted as inserts you would see 39000.

---

## Footguns

These are real and each will cost you a run.

**B1 — hive-style partitioning must be off.** `IncrementalLoader` never sets it, so Hudi 1.x
defaults it to `true`, and the rewrite job **rejects** such tables outright: hive-style needs
`_hoodie_partition_path` (`city=nyc`) to differ from the partition column (`nyc`), and the job
stamps one value into both. Pass `hive_style_partitioning=false` in **both** `--initial-options`
and `--options`.

**B2 — seed keys need an underscore.** Covered in step 1. Without
`suffixKeyWithPartitionPath: true`, keys gain rather than replace a suffix and lose homogeneity
with step-3 inserts.

**B3 — step 1 must use `hoodie.metadata.enable=false`, not just `record.index.enable=false`.**
If a metadata table exists from step 1, its `files` partition predates the fan-out. Hudi then
takes `listAllPartitionsFromMDT` instead of listing storage, and RLI is built from a stale
listing that has never heard of the new partitions — silently indexing only the seed.

**B4 — the fan-out must preserve instant time.** Hudi filters listed files by
`getCommitTime(fileName) <= latestCompletedInstant`. The rewrite job preserves the source instant,
so its files pass. A fresh higher instant not on the timeline would be **silently dropped** from
the index — no error, just an under-built RLI.

**B5 — this flow is spark-datasource only; do not pass `--api-type spark-sql`.** The commands
above omit `--api-type`, which defaults to `spark-datasource` — keep it that way. Two reasons:

- Hudi index and write configs are *write* configs. Under the SQL API they are routed into
  `TBLPROPERTIES`, so the RLI options in step 4 would not reliably take effect.
- `dropTableIfExists` (reached only from `tryCreateTable`, which requires
  `apiType == SparkSqlApi || format == Iceberg`) does, for Hudi, `DROP TABLE ... PURGE` **plus an
  unguarded `fs.delete(targetPath, true)`** — no `roundNo == 0` check, unlike the Iceberg/Delta
  branch beside it. Its guard is `roundNo == startRound`, which is *true* when resuming at round 1,
  so a spark-sql run would delete the bootstrapped table before loading it.

The datasource path never calls `tryCreateTable`, so neither applies here. (The delete is still
worth fixing upstream: resuming any Hudi run at a non-zero round via the SQL API destroys the
table.)

**B6 — never let round 0 reappear in step 4.** `roundNo == 0` forces `SaveMode.Overwrite` +
`bulk_insert`. Keep `--start-round >= 1`.

**B7 — set precombine explicitly.** `IncrementalLoader` never sets
`hoodie.datasource.write.precombine.field`, so with two updates to one key in a round the survivor
is arbitrary. Pass `precombine.field=ts` in both option sets.

## Known limitations

**Updates never touch newly-inserted records.** All rounds are generated in one datagen job, and
each round re-reads the same static table, so round 2 samples the *same* pre-round-1 key set as
round 1. Newly inserted keys are never update targets, and the two rounds' update sets overlap.

This makes RLI lookups easier than a real workload, where the live key set grows every commit. For
a correctness sanity check it is fine; for a *performance* number it understates index pressure.
Two ways out: interleave datagen and loading (generate round N → load round N → generate N+1), or
maintain an accumulating key set on disk that each round appends its new insert keys to.

**`nullifiedPartitions` does not compose with `externalBootstrap`.** It is silently ignored in
that mode rather than rejected — do not put it in a spec that uses `externalBootstrap`.

**One partition per rewrite-job invocation.** Fanning out to 1000 partitions means 1000
`spark-submit`s, each paying SparkSession startup. Multi-partition support is
[requested on the PR](https://github.com/onehouseinc/onehouse-dataplane/pull/2898).

## Scaling up

At real sizes the only changes are the numbers: seed partition record count, the partition list in
step 2's loop, and the per-partition counts in the step-3 spec. Also:

- Give each benchmark variant its own copy of the post-step-3 table (a bulk file copy — not a Hudi
  write) so baseline and variant start byte-identical.
- Size the seed partition to your target *per-partition* size; total table size is
  seed × partition count.
- `--parallelism` on the rewrite job bounds tasks per invocation; it defaults to one task per
  input file.
