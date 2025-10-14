package ai.onehouse.lakeloader

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import ai.onehouse.lakeloader.IncrementalLoader.ApiType
import ai.onehouse.lakeloader.StorageFormat.{Delta, Hudi, Iceberg, Parquet}
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ai.onehouse.lakeloader.utils.SparkUtils.{executeSparkSql, withPersisted}
import ai.onehouse.lakeloader.utils.StringUtils
import ai.onehouse.lakeloader.utils.StringUtils.lineSepBold
import io.delta.tables.DeltaTable

import java.io.Serializable
import scala.collection.mutable.ListBuffer

class IncrementalLoader(val spark: SparkSession,
                        val numRounds: Int = 10,
                        val catalogName: Option[String] = None,
                        val databaseName: String = "default") extends Serializable {

  private def tryCreateTable(schema: StructType,
                             outputPath: String,
                             format: StorageFormat,
                             opts: Map[String, String],
                             nonPartitioned: Boolean,
                             scenarioId: String): Unit = {

    val tableName = format match {
      case Hudi => genHudiTableName(scenarioId)
      case Iceberg => genIcebergTableName(scenarioId)
    }
    val escapedTableName = escapeTableName(tableName)
    val targetPath = format match {
      case Iceberg =>
        val simpleTableName = s"iceberg_$scenarioId"
        s"$outputPath/$databaseName/$simpleTableName"
      case _ => s"$outputPath/${tableName.replace('.', '/')}"
    }

    dropTableIfExists(format, escapedTableName, targetPath)

    val serializedOpts = opts.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
    val createTableSql = format match {
      case StorageFormat.Hudi =>
        s"""
           |CREATE TABLE $escapedTableName (
           |  ${schema.toDDL}
           |)
           |USING HUDI
           |TBLPROPERTIES (
           |  tableType = 'cow',
           |  primaryKey = '${opts(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key)}',
           |  preCombineField = '${opts(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key)}',
           |  ${serializedOpts}
           |)
           |LOCATION '$targetPath'
           |${if (nonPartitioned) "" else "PARTITIONED BY (partition)"}
           |""".stripMargin

      case StorageFormat.Iceberg =>
        s"""
           |CREATE TABLE $escapedTableName (
           |  ${schema.toDDL}
           |)
           |USING ICEBERG
           |TBLPROPERTIES (
           |  ${serializedOpts}
           |)
           |LOCATION '$targetPath'
           |${if (nonPartitioned) "" else "PARTITIONED BY (partition)"}
           |""".stripMargin

      case _ =>
        throw new UnsupportedOperationException(s"$format is not supported currently")
    }

    executeSparkSql(spark, createTableSql)
  }

  private def dropTableIfExists(format: StorageFormat, escapedTableName: String, targetPathStr: String): Unit = {
    format match {
      case StorageFormat.Iceberg =>
        // Since Iceberg persists its catalog information w/in the manifest it's sufficient to just
        // drop the table from SQL
        executeSparkSql(spark, s"DROP TABLE IF EXISTS $escapedTableName PURGE")

      case StorageFormat.Hudi =>
        executeSparkSql(spark, s"DROP TABLE IF EXISTS $escapedTableName PURGE")

        val targetPath = new Path(targetPathStr)
        val fs = targetPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        fs.delete(targetPath, true)
    }
  }

  def doWrites(inputPath: String,
               outputPath: String,
               parallelism: Int = 100,
               format: StorageFormat = Parquet,
               operation: OperationType = OperationType.Upsert,
               apiType: ApiType = ApiType.SparkDatasourceApi,
               opts: Map[String, String] = Map(),
               cacheInput: Boolean = false,
               overwrite: Boolean = true,
               nonPartitioned: Boolean = false,
               experimentId: String = StringUtils.generateRandomString(10),
               startRound: Int = 0): Unit = {
    println(
      s"""
         |$lineSepBold
         |Executing $experimentId ($numRounds rounds)
         |$lineSepBold
         |""".stripMargin)

    val allRoundTimes = new ListBuffer[Long]()
    (startRound until startRound + numRounds).foreach(roundNo => {
      println(
        s"""
           |$lineSepBold
           |Writing round ${roundNo - startRound + 1} / $numRounds (absolute round: $roundNo)
           |$lineSepBold
           |""".stripMargin)

      val saveMode = if (roundNo == 0 && overwrite) {
        SaveMode.Overwrite
      } else {
        SaveMode.Append
      }

      val targetOperation = if (roundNo == 0) {
        OperationType.Insert
      } else {
        operation
      }

      val inputDF =
        spark.read.format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
          .load(s"$inputPath/$roundNo")

      if (cacheInput) {
        inputDF.cache()
        println(s"Cached ${inputDF.count()} records from $inputPath")
      }

      // Some formats (like Iceberg) do require to create table in the Catalog before
      // you are able to ingest data into it
      if (roundNo == 0 && (apiType == ApiType.SparkSqlApi || format == StorageFormat.Iceberg)) {
        tryCreateTable(inputDF.schema, outputPath, format, opts, nonPartitioned, experimentId)
      }

      allRoundTimes += doWriteRound(inputDF, outputPath, parallelism, format, apiType, saveMode,
        targetOperation, opts, nonPartitioned, experimentId)

      inputDF.unpersist()
    })

    println(
      s"""
         |$lineSepBold
         |Total time taken by all rounds (${format}): ${allRoundTimes.sum}
         |Per round: ${allRoundTimes.toList}
         |$lineSepBold
         |""".stripMargin)
  }

  def doWriteRound(inputDF: DataFrame,
                   outputPath: String,
                   parallelism: Int = 2,
                   format: StorageFormat = Parquet,
                   apiType: ApiType = ApiType.SparkDatasourceApi,
                   saveMode: SaveMode = SaveMode.Append,
                   operation: OperationType = OperationType.Upsert,
                   opts: Map[String, String] = Map(),
                   nonPartitioned: Boolean = false,
                   experimentId: String): Long = {
    val startMs = System.currentTimeMillis()

    format match {
      case Hudi =>
        val tableName = genHudiTableName(experimentId)
        writeToHudi(inputDF, operation, outputPath, parallelism, apiType, saveMode, opts, nonPartitioned, tableName)
      case Delta =>
        val tableName = s"delta-$experimentId"
        writeToDelta(inputDF, operation, outputPath, parallelism, saveMode, nonPartitioned, tableName)
      case Parquet =>
        writeToParquet(inputDF, operation, outputPath, parallelism, saveMode, nonPartitioned)
      case Iceberg =>
        val tableName = genIcebergTableName(experimentId)
        writeToIceberg(inputDF, operation, nonPartitioned, tableName)
      case _ =>
        throw new UnsupportedOperationException(s"$format is not supported")
    }

    val timeTaken = System.currentTimeMillis() - startMs
    println(s"Took ${timeTaken} ms.")
    timeTaken
  }

  private def writeToIceberg(df: DataFrame,
                             operation: OperationType,
                             nonPartitioned: Boolean,
                             tableName: String): Unit = {
    val escapedTableName = escapeTableName(tableName)

    // NOTE: Iceberg requires incoming dataset to be partitioned, in case it's being ingested
    //       into partitioned table
    val repartitionedDF = df

    repartitionedDF.createOrReplaceTempView(s"source")

    operation match {
      case OperationType.Insert =>
        // NOTE: Iceberg requires ordering of the dataset when being inserted into partitioned tables
        // No caching needed for INSERT as it's a single-pass operation
        val insertIntoTableSql =
          s"""
             |INSERT INTO $escapedTableName
             |SELECT * FROM source ${if (nonPartitioned) "" else ""}
             |""".stripMargin

        executeSparkSql(spark, insertIntoTableSql)

      case OperationType.Upsert =>
        // Cache for MERGE operations as they require multiple passes over the data
        withPersisted(df) {
          df.createOrReplaceTempView(s"source")
          // Execute MERGE INTO performing
          //   - Updates for all records w/ matching (partition, key) tuples
          //   - Inserts for all remaining records
          executeSparkSql(spark,
            s"""
               |MERGE INTO $escapedTableName t
               |USING (SELECT * FROM source s)
               |ON t.key = s.key AND t.partition = s.partition
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
        }
    }
  }

  private def writeToDelta(df: DataFrame,
                           operation: OperationType,
                           outputPath: String,
                           parallelism: Int,
                           saveMode: SaveMode,
                           nonPartitioned: Boolean,
                           tableName: String): Unit = {
    val targetPath = s"$outputPath/$tableName"
    operation match {
      case OperationType.Insert =>
        val repartitionedDF = if (nonPartitioned) {
          df.repartition(parallelism)
        } else {
          df.repartition(parallelism, col("partition"))
        }

        val writer = repartitionedDF.write.format("delta")
        val partitionedWriter = if (nonPartitioned) {
          writer
        } else {
          writer.partitionBy("partition")
        }

        partitionedWriter
          .mode(saveMode)
          .save(targetPath)

      case OperationType.Upsert =>
        if (!DeltaTable.isDeltaTable(targetPath)) {
          throw new UnsupportedOperationException("Operation 'upsert' cannot be performed")
        } else {
          val deltaTable = DeltaTable.forPath(targetPath)
          deltaTable.as("oldData")
            .merge(
              df.as("newData"),
              "oldData.key = newData.key AND oldData.partition = newData.partition")
            .whenMatched
            .updateAll()
            .whenNotMatched
            .insertAll()
            .execute()
        }
    }
  }

  private def writeToParquet(df: DataFrame, operation: OperationType, outputPath: String, parallelism: Int, saveMode: SaveMode, nonPartitioned: Boolean): Unit = {
    operation match {
      case OperationType.Insert =>
        val repartitionedDF = if (nonPartitioned) {
          df.repartition(parallelism)
        } else {
          df.repartition(parallelism, col("partition"))
        }

        repartitionedDF.write
          .format("parquet")
          .mode(saveMode)
          .save(s"$outputPath/parquet")

      case OperationType.Upsert =>
        throw new UnsupportedOperationException("Operation 'upsert' is not supported for Parquet")
    }
  }

  private def writeToHudi(df: DataFrame,
                          operation: OperationType,
                          outputPath: String,
                          parallelism: Int,
                          apiType: ApiType,
                          saveMode: SaveMode,
                          opts: Map[String, String],
                          nonPartitioned: Boolean,
                          tableName: String): Unit = {
    // TODO cleanup
    val repartitionedDF = if (nonPartitioned) {
      df.repartition(parallelism)
    } else {
      df.repartition(parallelism, col("partition"))
    }

    apiType match {
      case ApiType.SparkDatasourceApi =>
        val targetOpts = opts ++ Map(
          HoodieWriteConfig.TBL_NAME.key() -> "hudi",
        )
        repartitionedDF.write.format("hudi")
          .options(targetOpts)
          .option(DataSourceWriteOptions.OPERATION.key, operation.toString)
          .mode(saveMode)
          .save(s"$outputPath/$tableName")

      case ApiType.SparkSqlApi =>
        repartitionedDF.createOrReplaceTempView("source")
        val escapedTableName = escapeTableName(tableName)

        operation match {
          case OperationType.Insert =>
            val insertIntoTableSql =
              s"""
                 |INSERT INTO $escapedTableName
                 |SELECT * FROM source
                 |""".stripMargin
            executeSparkSql(spark, insertIntoTableSql)
          case OperationType.Upsert =>
            // Execute MERGE INTO performing
            //   - Updates for all records w/ matching (partition, key) tuples
            //   - Inserts for all remaining records
            executeSparkSql(spark,
              s"""
                 |MERGE INTO $escapedTableName t
                 |USING (SELECT * FROM source s)
                 |ON s.key = t.key AND s.partition = t.partition
                 |WHEN MATCHED THEN UPDATE SET *
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin)
        }
    }
  }

  private def escapeTableName(tableName: String) =
    tableName.split('.').map(np => s"`$np`").mkString(".")

  private def genIcebergTableName(experimentId: String): String = {
    val tableName = s"iceberg_$experimentId"
    catalogName match {
      case Some(catalog) => s"$catalog.$databaseName.$tableName"
      case None => s"$databaseName.$tableName"
    }
  }

  private def genHudiTableName(experimentId: String): String =
    s"$databaseName.hudi-$experimentId".replace("-", "_")
}

sealed trait OperationType {
  def asString: String
}

object OperationType {
  case object Upsert extends OperationType { val asString = "upsert" }
  case object Insert extends OperationType { val asString = "insert" }

  def fromString(s: String): OperationType = s match {
    case "upsert" => Upsert
    case "insert" => Insert
    case _ => throw new IllegalArgumentException(s"Invalid OperationType: $s")
  }

  def values(): List[String] = List(Upsert.asString, Insert.asString)
}

sealed trait StorageFormat {
  def asString: String
}

object StorageFormat {
  case object Iceberg extends StorageFormat { val asString = "iceberg" }
  case object Delta extends StorageFormat { val asString = "delta" }
  case object Hudi extends StorageFormat { val asString = "hudi" }
  case object Parquet extends StorageFormat { val asString = "parquet" }

  def fromString(s: String): StorageFormat = s match {
    case "iceberg" => Iceberg
    case "delta" => Delta
    case "hudi" => Hudi
    case "parquet" => Parquet
    case _ => throw new IllegalArgumentException(s"Invalid StorageFormat: $s")
  }

  def values(): List[String] = List(Iceberg.asString, Delta.asString, Hudi.asString, Parquet.asString)
}

case class LoadConfig(numberOfRounds: Int = 10,
                      inputPath: String = "",
                      outputPath: String = "",
                      parallelism: Int = 100,
                      format: String = "hudi",
                      operationType: String = "upsert",
                      options: Map[String, String] = Map.empty,
                      nonPartitioned: Boolean = false,
                      experimentId: String = StringUtils.generateRandomString(10),
                      catalogName: Option[String] = None,
                      databaseName: String = "default",
                      startRound: Int = 0
                     )

object IncrementalLoader {
  // Enum for API types
  sealed trait ApiType { def asString: String }
  object ApiType {
    case object SparkDatasourceApi extends ApiType { val asString = "spark-datasource" }
    case object SparkSqlApi extends ApiType { val asString = "spark-sql" }
  }

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[LoadConfig]("lake-loader | incremental loader") {
      head("lake-loader", "1.x")

      opt[Int]("number-rounds")
        .action((x, c) => c.copy(numberOfRounds = x))
        .text("Number of rounds of incremental change data to generate. Default 10.")

      opt[String]('i', "input-path")
        .required()
        .action((x, c) => c.copy(inputPath = x))
        .text("Input path")

      opt[String]('o', "output-path")
        .required()
        .action((x, c) => c.copy(outputPath = x))
        .text("Output path")

      opt[Int]("parallelism")
        .required()
        .action((x, c) => c.copy(parallelism = x))
        .text("Parallelism")

      opt[String]("format")
        .required()
        .action((x, c) => c.copy(format = x))
        .validate { x =>
          if (StorageFormat.values().contains(x))
            Right(())
          else
            Left(s"Invalid format: '$x'. Allowed: ${StorageFormat.values().mkString(", ")}")
        }
        .text("Format to load data into. Options: " + StorageFormat.values().mkString(", "))

      opt[String]("operation-type")
        .action((x, c) => c.copy(operationType = x))
        .validate { x =>
          if (OperationType.values().contains(x))
            Right(())
          else
            Left(s"Invalid operation: '$x'. Allowed: ${OperationType.values().mkString(", ")}")
        }
        .text("Write operation type")

      opt[Map[String, String]]("options")
        .action((x, c) => c.copy(options = x))
        .text("Options")

      opt[Boolean]("non-partitioned")
        .action((x, c) => c.copy(nonPartitioned = x))
        .text("Non partitioned")

      opt[String]('e', "experiment-id")
        .action((x, c) => c.copy(experimentId = x))
        .text("Experiment ID")

      opt[String]("catalog")
        .action((x, c) => c.copy(catalogName = Some(x)))
        .text("Catalog name (optional, for Iceberg with catalogs)")

      opt[String]("database")
        .action((x, c) => c.copy(databaseName = x))
        .text("Database/schema/namespace name (default: 'default')")

      opt[Int]("start-round")
        .action((x, c) => c.copy(startRound = x))
        .text("Start round to load from (default: 0)")
    }

    parser.parse(args, LoadConfig()) match {
      case Some(config) =>
        val spark = SparkSession.builder
          .appName("lake-loader incremental data loader")
          .getOrCreate()

        val dataLoader = new IncrementalLoader(spark, config.numberOfRounds, config.catalogName, config.databaseName)
        dataLoader.doWrites(
          config.inputPath,
          config.outputPath,
          parallelism = config.parallelism,
          format = StorageFormat.fromString(config.format),
          operation = OperationType.fromString(config.operationType),
          opts = config.options,
          nonPartitioned = config.nonPartitioned,
          experimentId = config.experimentId,
          startRound = config.startRound
        )
        spark.stop()
      case None =>
        // scopt already prints help
        sys.exit(1)
    }
  }
}
