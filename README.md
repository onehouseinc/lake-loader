# Lake Loader

Lake loader is a tool to benchmark incremental load (writes) to data lakes and warehouses. The tool generates input datasets with configurations to cover different aspects of load patterns - number of records, number of partitions, record size, update to insert ratio, distribution of inserts & updates across partitions and total number of rounds of incremental loads to perform. 

The tool consists of two main components:

**Change data generator** This component takes a specified L pattern and generates rounds of inputs. Each input round has change records, which can be either an insert or an update to an insert in a prior input round.

**Incremental Loader** The loader component implements best practices for loading data into various open table formats using popular cloud data platforms like AWS EMR, Databricks and Snowflake. Round 0 is specially designed to perform a one-time bulk load using the preferred bulk loading methods for the data platform. Round 1 and above simply perform incremental loads using pre-generated input change records from each round.

![Figure: Shows the Lake Loader tool's high-level functioning to benchmark incremental loads across popular cloud data platforms.
](src/main/resources/images/lakeLoaderArch.png)


### Building

Lake loader is built with Java 8, 11 or 17 using Maven.

* mvn clean package

### Engine Compatibility

Lake loader requires spark to generate and load the dataset, compatible with spark3.x including spark3.5

### Instructions to generate dataset
In the following sections, we write down the instantiations of the  ChangeDataGenerator class to 
generate datasets that can produce **FACT** tables (Zipfian pattern for updates), **DIM** tables (random pattern for updates) and **EVENTS** table (Append-only table).

**NOTE** Instructions provided are for Spark-Shell, but Spark-Submit can be used with the application class name ChangeDataGenerator.

#### Fact table arguments
The following generates dateset for a 1TB **FACT** table with 
- 365 partitions based on date
- 1B records
- 40 column schema
- record size of almost 1KB
- 25% of an incremental batch is updates, 75% are new records.
- updates are distributed across partitions in a zipf distribution, with most updates in the latest partitions.
- updates are spread across last 90 partitions, while insert records are written to last 2 partitions with equal spread.

```
import ai.onehouse.lakeloader.ChangeDataGenerator
import ai.onehouse.lakeloader.ChangeDataGenerator.UpdatePatterns

val input_path = "file:///<input_path>"
val numRounds = 20

val partitionDistribution:List[List[Double]] = List(List.fill(365)(1.0/365)) ++ List.fill(numRounds-1)(List(0.5, 0.5) ++ List.fill(363)(0.0))

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(input_path,
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
import ai.onehouse.lakeloader.ChangeDataGenerator.UpdatePatterns

val input_path = "file:///<input_path>"
val numRounds = 20

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(input_path,
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
import ai.onehouse.lakeloader.ChangeDataGenerator.UpdatePatterns

val input_path = "file:///<input_path>"
val numRounds = 20
val numPartitions = 1095

val partitionDistribution:List[List[Double]] = List(List.fill(numPartitions)(1.0/numPartitions)) ++ List.fill(numRounds-1)(List(1.0) ++ List.fill(numPartitions-1)(0.0))

val datagen = new ChangeDataGenerator(spark, numRounds = numRounds)
datagen.generateWorkload(input_path,
                         roundsDistribution = List(10000000000L) ++ List.fill(numRounds-1)(50000000L),
                         numColumns = 30,
                         recordSize = 200,
                         updateRatio = 0.0f,
                         totalPartitions = numPartitions,
                         partitionDistributionMatrixOpt = Some(partitionDistribution))
```

### Instructions to load dataset incrementally across different platforms

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
import ai.onehouse.lakeloader.StorageFormat
import ai.onehouse.lakeloader.OperationType

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
import ai.onehouse.lakeloader.StorageFormat
import ai.onehouse.lakeloader.OperationType

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
import ai.onehouse.lakeloader.StorageFormat
import ai.onehouse.lakeloader.OperationType

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
import ai.onehouse.lakeloader.StorageFormat
import ai.onehouse.lakeloader.OperationType

val experimentId = "dbr_fact"
val numRounds = 10
val inputPath = "s3a://input/fact-data"
val targetPath = "s3a://output/dbr-fact"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  parallelism = 1000,
  format = StorageFormat.Delta,
  operation = OperationType.Upsert,
  opts = opts,
  experimentId = experimentId)
```

Run the following for DIM tables since they are non-partitioned.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.StorageFormat
import ai.onehouse.lakeloader.OperationType

val experimentId = "dbr_dim"
val numRounds = 10
val inputPath = "s3a://input/dim-data"
val targetPath = "s3a://output/dbr-dim"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  parallelism = 1000,
  format = StorageFormat.Delta,
  operation = OperationType.Upsert,
  opts = opts,
  nonPartitioned = true,
  experimentId = experimentId)
```

Run the following for EVENT tables since they are insert-only.
```
import ai.onehouse.lakeloader.IncrementalLoader
import ai.onehouse.lakeloader.StorageFormat
import ai.onehouse.lakeloader.OperationType

val experimentId = "dbr_event"
val numRounds = 10
val inputPath = "s3a://input/event-data"
val targetPath = "s3a://output/dbr-event"

val loader = new IncrementalLoader(spark, numRounds = numRounds)

var opts = Map("write.parquet.compression-codec" -> "snappy")
loader.doWrites(inputPath,
  targetPath,
  parallelism = 1000,
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