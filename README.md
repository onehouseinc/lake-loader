# Lake Loader

Lake loader is a tool to benchmark incremental load (writes) to data lakes and warehouses. The tool generates input datasets with configurations to cover different aspects of load patterns - number of records, number of partitions, record size, update to insert ratio, distribution of inserts & updates across partitions and total number of rounds of incremental loads to perform. 

The tool consists of two main components:

**Change data generator** This component takes a specified L pattern and generates rounds of inputs. Each input round has change records, which can be either an insert or an update to an insert in a prior input round.

**Incremental Loader** The loader component implements best practices for loading data into various open table formats using popular cloud data platforms like AWS EMR, Databricks and Snowflake. Round 0 is specially designed to perform a one-time bulk load using the preferred bulk loading methods for the data platform. Round 1 and above simply perform incremental loads using pre-generated input change records from each round.

![Figure: Shows the Lake Loader tool's high-level functioning to benchmark incremental loads across popular cloud data platforms.
](src/main/resources/images/lakeLoaderArch.png)


### Building

Lake loader is built with Java 11 or 17 using Maven.

Run `mvn clean package` to build.

### Formatting

Run `mvn scalafmt:format` to apply scala formatting.

### Engine Compatibility

Lake loader requires spark to generate and load the dataset, compatible with spark3.x including spark3.5

## Component Overview

Lake Loader provides two main ways to use its components:

1. **Scala API**: Use the components programmatically within spark-shell or notebooks by importing the classes and calling methods
2. **Command-Line Interface (CLI)**: Use spark-submit with command-line arguments for automated scripts and workflows

Both interfaces support the same parameters and functionality. The parameter reference tables below show both the Scala API parameter names and CLI flags.

## ChangeDataGenerator Parameters

The ChangeDataGenerator component generates change data with configurable patterns for benchmarking.

**Scala API:**
```scala
// Constructor
new ChangeDataGenerator(spark: SparkSession, numRounds: Int)

// Main method
def generateWorkload(
  outputPath: String,
  roundsDistribution: List[Long],
  numColumns: Int = 10,
  recordSize: Int = 1024,
  updateRatio: Float = 0.5f,
  totalPartitions: Int = -1,
  partitionDistributionMatrixOpt: Option[List[List[Double]]] = None,
  datagenFileSize: Int = 128 * 1024 * 1024,
  skipIfExists: Boolean = false,
  startRound: Int = 0,
  primaryKeyType: KeyType = KeyType.Random,
  updatePatterns: UpdatePatterns = UpdatePatterns.Uniform,
  numPartitionsToUpdate: Int = -1,
  zipfianShape: Double = 2.93
)
```

**CLI:**
```bash
spark-submit --class ai.onehouse.lakeloader.ChangeDataGenerator <jar-file> [options]
```

### Parameter Reference

| Parameter             | CLI Flag                               | Type           | Default    | Description                                                     |
|-----------------------|----------------------------------------|----------------|------------|-----------------------------------------------------------------|
| outputPath            | `-p`, `--path`                         | String         | *required* | Output path where generated change data will be stored          |
| numRounds             | `--number-rounds`                      | Int            | 10         | Number of rounds of incremental change data to generate         |
| N/A (CLI only)        | `--number-records-per-round`           | Long           | 1000000    | Number of records per round (CLI only, uniform across rounds)   |
| roundsDistribution    | N/A (CLI only)                         | List[Long]     | *computed* | Number of records per round (Scala API only, per-round control) |
| numColumns            | `--number-columns`                     | Int            | 10         | Number of columns in schema of generated data (min: 5)          |
| recordSize            | `--record-size`                        | Int            | 1024       | Record size of generated data in bytes                          |
| updateRatio           | `--update-ratio`                       | Double         | 0.5        | Ratio of updates to total records (0.0-1.0)                     |
| totalPartitions       | `--total-partitions`                   | Int            | -1         | Total number of partitions (-1 for unpartitioned)               |
| datagenFileSize       | `--datagen-file-size`                  | Int            | 134217728  | Target data file size in bytes (default: 128MB)                 |
| skipIfExists          | `--skip-if-exists`                     | Boolean        | false      | Skip generation if folder already exists                        |
| startRound            | `--start-round`                        | Int            | 0          | Starting round number (for resuming generation)                 |
| primaryKeyType        | `--primary-key-type`                   | KeyType        | Random     | Key generation type: `Random`, `TemporallyOrdered`              |
| updatePatterns        | `--update-pattern`                     | UpdatePatterns | Uniform    | Update distribution: `Uniform`, `Zipf`                          |
| numPartitionsToUpdate | `--num-partitions-to-update`           | Int            | -1         | Number of partitions to update (-1 for all)                     |
| zipfianShape          | `--zipfian-shape`                      | Double         | 2.93       | Shape parameter for Zipf distribution (higher = more skewed)    |

**Notes**:
* **Record count specification**: CLI uses `--number-records-per-round` which applies a uniform count across all rounds. Scala API uses `roundsDistribution` parameter which allows specifying different record counts for each round (e.g., `List(1000000000L, 10000000L, 10000000L, ...)` for a large initial load followed by smaller incremental rounds).

## IncrementalLoader Parameters

The IncrementalLoader component loads the generated change data into various table formats.

**Scala API:**
```scala
// Constructor
new IncrementalLoader(spark: SparkSession, numRounds: Int)

// Main method
def doWrites(
  inputPath: String,
  outputPath: String,
  format: StorageFormat = StorageFormat.Hudi,
  operation: OperationType = OperationType.Upsert,
  apiType: ApiType = ApiType.SparkDatasourceApi,
  opts: Map[String, String] = Map.empty,
  nonPartitioned: Boolean = false,
  experimentId: String = generateRandomString(10),
  startRound: Int = 0,
  catalog: String = "spark_catalog",
  database: String = "default",
  mergeConditionColumns: Seq[String] = Seq.empty,
  mergeMode: MergeMode = MergeMode.UpdateInsert,
  updateColumns: Seq[String] = Seq.empty
)
```

**CLI:**
```bash
spark-submit --class ai.onehouse.lakeloader.IncrementalLoader <jar-file> [options]
```

### Parameter Reference

| Parameter             | CLI Flag                               | Type               | Default          | Description                                               |
|-----------------------|----------------------------------------|--------------------|------------------|-----------------------------------------------------------|
| inputPath             | `-i`, `--input-path`                   | String             | *required*       | Input path where change data was generated                |
| outputPath            | `-o`, `--output-path`                  | String             | *required*       | Output path where table data will be written              |
| numRounds             | `--number-rounds`                      | Int                | 10               | Number of rounds of incremental data to load              |
| format                | `--format`                             | StorageFormat      | hudi             | Table format: `iceberg`, `delta`, `hudi`, `parquet`       |
| operation             | `--operation-type`                     | OperationType      | upsert           | Write operation: `upsert`, `insert`                       |
| apiType               | `--api-type`                           | ApiType            | spark-datasource | API type: `spark-datasource`, `spark-sql`                 |
| opts                  | `--options`                            | Map[String,String] | {}               | Format-specific options (e.g., `key1=value1 key2=value2`) |
| nonPartitioned        | `--non-partitioned`                    | Boolean            | false            | Whether table is non-partitioned                          |
| experimentId          | `-e`, `--experiment-id`                | String             | *random*         | Experiment identifier for tracking                        |
| startRound            | `--start-round`                        | Int                | 0                | Starting round for loading (for resuming)                 |
| catalog               | `--catalog`                            | String             | spark_catalog    | Catalog name for table registration                       |
| database              | `--database`                           | String             | default          | Database name for table registration                      |
| mergeConditionColumns | `--additional-merge-condition-columns` | Seq[String]        | []               | Additional merge condition columns beyond defaults        |
| mergeMode             | `--merge-mode`                         | MergeMode          | update-insert    | Merge mode: `update-insert`, `delete-insert`              |
| updateColumns         | `--update-columns`                     | Seq[String]        | []               | Specific columns to update (empty = all columns)          |

**Notes**:
* CLI uses `--additional-merge-condition-columns` while Scala API uses `mergeConditionColumns` for the full merge condition list. Default merge columns are `[key]` for non-partitioned or `[key, partition]` for partitioned tables.
* The `--api-type` option is only supported with `--format hudi`. Using `spark-sql` with other formats will result in an error.

## Usage Examples

### Dataset Generation Examples

This section demonstrates how to use the ChangeDataGenerator to create datasets for different table types: **FACT** tables (Zipfian pattern for updates), **DIM** tables (random pattern for updates), and **EVENTS** tables (Append-only).

**NOTE**: Examples below use Scala API in spark-shell. For CLI usage with spark-submit, use the class name `ai.onehouse.lakeloader.ChangeDataGenerator` and refer to the parameter table above for command-line flags.

#### Fact table arguments
The following generates dataset for a 1TB **FACT** table with:
- 365 partitions based on date
- 1B records
- 40 column schema
- record size of almost 1KB
- 25% of an incremental batch is updates, 75% are new records.
- updates are distributed across partitions in a zipf distribution, with most updates in the latest partitions.
- updates are spread across last 90 partitions, while insert records are written to last 2 partitions with equal spread.

```
import ai.onehouse.lakeloader.ChangeDataGenerator
import ai.onehouse.lakeloader.configs.UpdatePatterns

val output_path = "file:///<output_path>"
val numRounds = 20

val partitionDistribution:List[List[Double]] = List(List.fill(365)(1.0/365)) ++ List.fill(numRounds-1)(List(0.5, 0.5) ++ List.fill(363)(0.0))

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(output_path,
                         roundsDistribution = List(1000000000L) ++ List.fill(numRounds-1)(10000000L),
                         numColumns = 40,
                         recordSize = 1000,
                         updateRatio = 0.25f,
                         totalPartitions = 365,
                         partitionDistributionMatrixOpt = Some(partitionDistribution),
                         updatePatterns=UpdatePatterns.Zipf,
                         numPartitionsToUpdate=90)
)
```

#### Dim table arguments
The following generates dateset for a 100GB **DIM** table with
- No partitions or un-partitioned table
- 100M records
- 99 column schema
- record size of almost 1KB
- 50% of an incremental batch is updates, 50% are new records.
- updates are randomly distributed across the entire dataset.

```
import ai.onehouse.lakeloader.ChangeDataGenerator
import ai.onehouse.lakeloader.configs.UpdatePatterns

val output_path = "file:///<input_path>"
val numRounds = 20

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(output_path,
                         roundsDistribution = List(100000000L) ++ List.fill(numRounds-1)(500000L),
                         numColumns = 99,
                         recordSize = 1000,
                         updateRatio = 0.5f,
                         totalPartitions = 1,
                         updatePatterns = UpdatePatterns.Uniform,
                         numPartitionsToUpdate = 1)
```

#### Events table arguments
The following generates dateset for a 2TB **EVENTS** table with
- 1095 partitions based on date
- 10B records
- 30 column schema
- record size of almost 200Bytes
- 100% of incremental batch are new records (No updates).
- All records in an incremental batch will be generated for the latest partition.

```
import ai.onehouse.lakeloader.ChangeDataGenerator
import ai.onehouse.lakeloader.configs.UpdatePatterns

val output_path = "file:///<output_path>"
val numRounds = 20
val numPartitions = 1095

val partitionDistribution:List[List[Double]] = List(List.fill(numPartitions)(1.0/numPartitions)) ++ List.fill(numRounds-1)(List(1.0) ++ List.fill(numPartitions-1)(0.0))

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(output_path,
                         roundsDistribution = List(10000000000L) ++ List.fill(numRounds-1)(50000000L),
                         numColumns = 30,
                         recordSize = 200,
                         updateRatio = 0.0f,
                         totalPartitions = numPartitions,
                         partitionDistributionMatrixOpt = Some(partitionDistribution))
```

### Incremental Loading Examples

This section shows how to use the IncrementalLoader component to load the generated change data into various table formats.

**NOTE**: Examples below use Scala API in spark-shell. For CLI usage with spark-submit, use the class name `ai.onehouse.lakeloader.IncrementalLoader` and refer to the parameter table above for command-line flags.

#### EMR
Provision a cluster with the latest EMR version 7.5.0. 
Using the spark-shell, you need to configure the following additional configs besides the typical spark configs:
```
  --jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar
  --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my_catalog.warehouse=s3://hudi-benchmark-source/icebergCatalog/ \
  --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.defaultCatalog=my_catalog \
  --conf spark.sql.catalog.my_catalog.http-client.apache.max-connections=5000 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Within spark-shell, run the following command for FACT tables.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.configs.StorageFormat
import ai.onehouse.lakeloader.configs.OperationType

val experimentId = "emr_fact"
val numRounds = 10
val inputPath = "s3a://input/fact-data"
val targetPath = "s3a://output/emr-fact"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  format = StorageFormat.Iceberg,
  operation = OperationType.Upsert,
  opts = opts,
  experimentId = experimentId)
```

Run the following for DIM tables since they are non-partitioned.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.configs.StorageFormat
import ai.onehouse.lakeloader.configs.OperationType

val experimentId = "emr_dim"
val numRounds = 10
val inputPath = "s3a://input/dim-data"
val targetPath = "s3a://output/emr-dim"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  format = StorageFormat.Iceberg,
  operation = OperationType.Upsert,
  opts = opts,
  nonPartitioned = true,
  experimentId = experimentId)
```

Run the following for EVENT tables since they are insert-only.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.configs.StorageFormat
import ai.onehouse.lakeloader.configs.OperationType

val experimentId = "emr_event"
val numRounds = 10
val inputPath = "s3a://input/event-data"
val targetPath = "s3a://output/emr-event"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  format = StorageFormat.Iceberg,
  operation = OperationType.Insert,
  opts = opts,
  experimentId = experimentId)
```

#### Databricks
Provision a recent Databricks cluster with or without native acceleration (photon) enabled.
Use the following (optional) spark configs to disable deletion vectors for delta to test with COPY_ON_WRITE,
snappy compression and disable parquet file caching.
```
  .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
  .config("spark.sql.parquet.compression.codec", "snappy")
  .config("spark.databricks.io.cache.enabled", "false")
```
Within a notebook, run the following command for FACT tables.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.configs.StorageFormat
import ai.onehouse.lakeloader.configs.OperationType

val experimentId = "dbr_fact"
val numRounds = 10
val inputPath = "s3a://input/fact-data"
val targetPath = "s3a://output/dbr-fact"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  format = StorageFormat.Delta,
  operation = OperationType.Upsert,
  opts = opts,
  experimentId = experimentId)
```

Run the following for DIM tables since they are non-partitioned.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.configs.StorageFormat
import ai.onehouse.lakeloader.configs.OperationType

val experimentId = "dbr_dim"
val numRounds = 10
val inputPath = "s3a://input/dim-data"
val targetPath = "s3a://output/dbr-dim"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  format = StorageFormat.Delta,
  operation = OperationType.Upsert,
  opts = opts,
  nonPartitioned = true,
  experimentId = experimentId)
```

Run the following for EVENT tables since they are insert-only.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.configs.StorageFormat
import ai.onehouse.lakeloader.configs.OperationType

val experimentId = "dbr_event"
val numRounds = 10
val inputPath = "s3a://input/event-data"
val targetPath = "s3a://output/dbr-event"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  format = StorageFormat.Delta,
  operation = OperationType.Insert,
  opts = opts,
  experimentId = experimentId)
```

#### Snowflake
In the case of Snowflake warehouse, we have added 3 scripts to help run the FACT, DIM and EVENT table benchmarks 
using a medium cluster size. The files are under the *scripts/snowflake* directory
- snowflake_fact_incremental_load_sql.txt
- snowflake_dim_incremental_load_sql.txt
- snowflake_events_incremental_load_sql.txt
