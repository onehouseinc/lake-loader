# Runbook: 1TB FACT — batch-1 bootstrap, batch-2 enable global RLI

## Workload summary
- **Batch 1**: bulk-insert ~1 TB across 365 partitions, no RLI.
- **Batch 2**: upsert 100 records into a single partition, with **global RLI enabled** for the first time. Hudi will bootstrap the RLI partition from the existing base files on this commit.

## Why this works in one invocation
`IncrementalLoader` accepts two distinct option maps and applies them per round (`IncrementalLoader.scala:315`):
- `--initial-options` → round 0 only
- `--options` → rounds 1+

So you can run a single 2-round job: round 0 writes without RLI, round 1 turns RLI on and Hudi auto-bootstraps it.

---

## Step 1 — Sizing

For 1 TB / 365 partitions at the default `--record-size=1024` bytes:
- Records ≈ `1,099,511,627,776 / 1024 ≈ 1,073,741,824` (~1.07 B records).
- Round 1 is exactly **100 records** in **1 partition**.

`--number-records-per-round 1073741824,100` and `--num-partitions-to-update 1` give the right split.

To force the round-1 updates into a single partition, use `--partition-distribution` with two segments separated by `;`. Round 0 = uniform over all 365 partitions; round 1 = all weight on one partition.

---

## Step 2 — Generate the change data

```bash
spark-submit \
  --class ai.onehouse.lakeloader.ChangeDataGenerator \
  <lake-loader.jar> \
  -p s3://bucket/lakeloader/input/fact_1tb \
  --number-rounds 2 \
  --number-records-per-round 1073741824,100 \
  --total-partitions 365 \
  --num-partitions-to-update 1 \
  --update-ratio 1.0 \
  --partition-distribution "$(python3 -c 'print(",".join(["1"]*365) + ";1" + ",0"*364)')" \
  --record-size 1024 \
  --primary-key-type TemporallyOrdered \
  --datagen-file-size 134217728
```

Notes:
- `--update-ratio 1.0` makes all 100 round-1 records hit existing keys (true updates). Drop to `0.5` if you want a mix.
- The python one-liner emits `1,1,…,1;1,0,0,…,0`: uniform in round 0, concentrated on partition 0 in round 1.
- `--primary-key-type TemporallyOrdered` keeps keys ordered so the RLI build later is deterministic.

---

## Step 3 — Decide the Hudi options

### Round 0 — `--initial-options` (RLI OFF)
Keep metadata table on (defaults), no RLI yet:
```
hoodie.metadata.enable=true
hoodie.metadata.record.index.enable=false
```

### Round 1 — `--options` (global RLI ON, dynamic file groups, growth factor 1.0)
```
hoodie.metadata.enable=true
hoodie.metadata.record.index.enable=true
hoodie.index.type=RECORD_INDEX
hoodie.metadata.record.index.growth.factor=1.0
```

Behavior you'll get:
- **Bootstrap**: When `record.index.enable` flips to `true` on an existing table, the metadata writer scans existing base files and populates the `record_index` MDT partition during this commit. This is native Hudi behavior — no lake-loader flag needed.
- **Global RLI**: Setting `hoodie.index.type=RECORD_INDEX` makes the writer use the metadata-table RLI for index lookups. RLI is global by construction (key → fileId across all partitions), so no separate `global` flag exists.
- **Dynamic file groups**: Leave `hoodie.metadata.record.index.min.filegroup.count` / `max.filegroup.count` unset — Hudi sizes the RLI partition from the bootstrap key count using its built-in estimator.
- **Growth factor 1.0**: `hoodie.metadata.record.index.growth.factor=1.0` (no headroom over the bootstrap estimate).
- **HFile size**: leave `hoodie.metadata.record.index.max.file.size` (and the other MDT HFile-size knobs) at defaults per your ask.

---

## Step 4 — Run the loader (single invocation, 2 rounds)

```bash
spark-submit \
  --class ai.onehouse.lakeloader.IncrementalLoader \
  <lake-loader.jar> \
  -i s3://bucket/lakeloader/input/fact_1tb \
  -o s3://bucket/lakeloader/output \
  --format hudi \
  --write-mode copy-on-write \
  --number-rounds 2 \
  --initial-operation-type bulk_insert \
  --operation-type upsert \
  --record-key-field key \
  --partition-path-field partition \
  --initial-options \
      hoodie.metadata.enable=true,\
hoodie.metadata.record.index.enable=false \
  --options \
      hoodie.metadata.enable=true,\
hoodie.metadata.record.index.enable=true,\
hoodie.index.type=RECORD_INDEX,\
hoodie.metadata.record.index.growth.factor=1.0
```

`scopt` parses `--options` / `--initial-options` as a comma-separated `key=val` map; do not wrap them in quotes per-entry. The `\` line continuations keep them on one logical argument.

---

## Step 5 — Verify

After the run, confirm RLI was bootstrapped on commit 2:

1. List MDT partitions — `record_index` should be present:
   ```
   aws s3 ls s3://bucket/lakeloader/output/<table>/.hoodie/metadata/
   ```
2. Inspect the second commit's metadata — `<ts>.deltacommit` under `.hoodie/metadata/.hoodie/` should show writes into the `record_index` partition with a file-group count ≈ (total keys × 1.0 / keys-per-fg). With ~1.07 B keys at the default ~50 K keys per HFile group, expect on the order of ~20 K RLI file groups (rough — Hudi's estimator decides).
3. Spot-check a query that filters by `key` and confirm the plan reads RLI (Spark UI → SQL plan → "RecordIndex" pruning, or Hudi driver logs `Loaded record index ...`).

---

## Gotchas

- **Don't drop the table between rounds.** The loader does drop the Hudi table when `roundNo == 0` (`IncrementalLoader.scala:153`), which is fine — that's round 0. Round 1 reuses it, which is what enables the bootstrap.
- **Async compaction**: not needed for COW. Leave `--async-compaction` off.
- **Concurrency options** are only auto-injected for MOR + async-compaction. Single-writer COW needs nothing extra.
- **Growth factor key** — Hudi 1.1.x uses `hoodie.metadata.record.index.growth.factor`. If you see RLI sized larger than expected, double-check this key landed (it's surfaced as `Loaded record index ... growth factor=1.0` in driver logs).
- The lake-loader CLI has no dedicated RLI flag; everything goes through `--initial-options` / `--options`. That's intentional — it's a generic Hudi knob passthrough.
