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

import ai.onehouse.lakeloader.configs.MergeMode.{DeleteInsert, UpdateInsert}
import ai.onehouse.lakeloader.configs.{ApiType, LoadConfig, MergeMode, OperationType, StorageFormat}
import ai.onehouse.lakeloader.configs.StorageFormat.{Delta, Hudi, Iceberg, Parquet}
import ai.onehouse.lakeloader.parser.IncrementalLoaderParser
import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ai.onehouse.lakeloader.utils.SparkUtils.executeSparkSql
import ai.onehouse.lakeloader.utils.StringUtils
import ai.onehouse.lakeloader.utils.StringUtils.lineSepBold
import io.delta.tables.DeltaTable

import java.io.Serializable
import scala.collection.mutable.ListBuffer

class IncrementalLoader(
    val spark: SparkSession,
    val numRounds: Int = 10,
    val catalog: String = "spark_catalog",
    val database: String = "default")
    extends Serializable {

  private def updateIcebergTable(
                              schema: StructType,
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

    val updateQuery = s"ALTER TABLE $escapedTableName SET TBLPROPERTIES ('write.distribution-mode' = 'hash')"
    executeSparkSql(spark, updateQuery)
}

  private def createIcebergTableForSpj(
                              schema: StructType,
                              outputPath: String,
                              opts: Map[String, String],
                              nonPartitioned: Boolean,
                              scenarioId: String,
                              rawDF: DataFrame): DataFrame = {
    println("Creating Table for SPJ")
    val icebergRawTableNameForSpj = genIcebergRawTableNameForSpj(scenarioId)
    val escapedIcebergRawTableNameForSpj = escapeTableName(icebergRawTableNameForSpj)
    val targetPathForSpj = s"$outputPath/${icebergRawTableNameForSpj.replace('.', '/')}"
    val serializedOpts = opts.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
    executeSparkSql(spark, s"DROP TABLE IF EXISTS $escapedIcebergRawTableNameForSpj PURGE")
    val startMs = System.currentTimeMillis()
    val icebergRawTableDDLForSpj = (s"""
                                       |CREATE TABLE  $escapedIcebergRawTableNameForSpj (
                                       |  ${schema.toDDL}
                                       |)
                                       |USING ICEBERG
                                       |${if (serializedOpts.nonEmpty) s"TBLPROPERTIES (\n  ${serializedOpts}\n)" else ""}
                                       |LOCATION '$targetPathForSpj'
                                       |${if (nonPartitioned) "" else "PARTITIONED BY (partition)"}
                                       |""".stripMargin)
    executeSparkSql(spark, icebergRawTableDDLForSpj)
    rawDF.createOrReplaceTempView("rawIcebergDF")
    val insertSQL = s"""INSERT INTO $escapedIcebergRawTableNameForSpj SELECT * FROM rawIcebergDF"""
    val timeTaken = System.currentTimeMillis() - startMs
    println(s"SPJ Raw Table Took ${timeTaken} ms.")
    executeSparkSql(spark, insertSQL)
    spark.sql(s"""SELECT * FROM $escapedIcebergRawTableNameForSpj""")

  }

  private def tryCreateTable(
      schema: StructType,
      outputPath: String,
      format: StorageFormat,
      opts: Map[String, String],
      nonPartitioned: Boolean,
      scenarioId: String,
      roundNum: Int): Unit = {

    val tableName = format match {
      case Hudi => genHudiTableName(scenarioId)
      case Iceberg => genIcebergTableName(scenarioId)
    }
    val escapedTableName = escapeTableName(tableName)
    val targetPath = s"$outputPath/${tableName.replace('.', '/')}"

    dropTableIfExists(format, escapedTableName, targetPath, roundNum)

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
           |CREATE TABLE IF NOT EXISTS $escapedTableName (
           |  ${schema.toDDL}
           |)
           |USING ICEBERG
           |${if (serializedOpts.nonEmpty) s"TBLPROPERTIES (\n  ${serializedOpts}\n)" else ""}
           |LOCATION '$targetPath'
           |${if (nonPartitioned) "" else "PARTITIONED BY (partition)"}
           |""".stripMargin

      case _ =>
        throw new UnsupportedOperationException(s"$format is not supported currently")
    }

    executeSparkSql(spark, createTableSql)
  }

  private def dropTableIfExists(
      format: StorageFormat,
      escapedTableName: String,
      targetPathStr: String,
      roundNum: Int): Unit = {
    format match {
      case StorageFormat.Iceberg =>
        // Since Iceberg persists its catalog information w/in the manifest it's sufficient to just
        // drop the table from SQL
        if(roundNum == 0){
          executeSparkSql(spark, s"DROP TABLE IF EXISTS $escapedTableName PURGE")
        }
      case StorageFormat.Hudi =>
        executeSparkSql(spark, s"DROP TABLE IF EXISTS $escapedTableName PURGE")

        val targetPath = new Path(targetPathStr)
        val fs = targetPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

        fs.delete(targetPath, true)
    }
  }

  private def buildMergeCondition(mergeColumns: Seq[String]): String = {
    mergeColumns.map(col => s"t.$col = s.$col").mkString(" AND ")
  }

  private def buildUpdateSetClause(updateColumns: Seq[String]): String = {
    if (updateColumns.isEmpty) {
      "*"
    } else {
      updateColumns.map(col => s"$col = s.$col").mkString(", ")
    }
  }

  private def buildUpdateExprMap(updateColumns: Seq[String]): Map[String, String] = {
    if (updateColumns.isEmpty) {
      Map.empty
    } else {
      updateColumns.map(col => col -> s"newData.$col").toMap
    }
  }

  private def enableStoragePartitionedJoin(): Unit = {
    println("Enabling Storage Partitioned Join for Iceberg MERGE.")
    spark.conf.set("spark.sql.iceberg.distribution-mode", "none")
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
    spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
    spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
    spark.conf.set("spark.sql.requireAllClusterKeysForCoPartition", "false")
    spark.conf.set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", "true")
  }

  def doWrites(
      inputPath: String,
      outputPath: String,
      format: StorageFormat = Parquet,
      operation: OperationType = OperationType.Upsert,
      initialOperation: OperationType = OperationType.Upsert,
      apiType: ApiType = ApiType.SparkDatasourceApi,
      opts: Map[String, String] = Map(),
      incrOpts: Map[String, String] = Map(),
      cacheInput: Boolean = false,
      overwrite: Boolean = true,
      nonPartitioned: Boolean = false,
      experimentId: String = StringUtils.generateRandomString(10),
      startRound: Int = 0,
      mergeConditionColumns: Seq[String] = Seq("key", "partition"),
      updateColumns: Seq[String] = Seq.empty,
      mergeMode: MergeMode = MergeMode.UpdateInsert,
      icebergSpjEnable: Boolean): Unit = {
    require(inputPath.nonEmpty, "Input path cannot be empty")
    require(outputPath.nonEmpty, "Output path cannot be empty")
    println(s"""
         |$lineSepBold
         |Executing $experimentId ($numRounds rounds)
         |$lineSepBold
         |""".stripMargin)

    val allRoundTimes = new ListBuffer[Long]()
    (startRound until startRound + numRounds).foreach(roundNo => {
      println(s"""
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
        if (format == Hudi) {
          // Use the bulk insert operation for Hudi's first batch
          OperationType.BulkInsert
        } else {
          OperationType.Insert
        }
      } else {
        operation
      }

      val rawDF =
        spark.read
          .format(ChangeDataGenerator.DEFAULT_DATA_GEN_FORMAT)
          .load(s"$inputPath/$roundNo")

      if (cacheInput) {
        rawDF.cache()
        println(s"Cached ${rawDF.count()} records from $inputPath")
      }

      // Some formats (like Iceberg) do require to create table in the Catalog before
      // you are able to ingest data into it
      if (roundNo == startRound && (apiType == ApiType.SparkSqlApi || format == StorageFormat.Iceberg)) {
        tryCreateTable(rawDF.schema, outputPath, format, opts, nonPartitioned, experimentId, roundNo)
      }

      // Want to reset the write distribution mode to hash after first commit.
      if (roundNo == 1 && format == StorageFormat.Iceberg && !nonPartitioned) {
        updateIcebergTable(rawDF.schema, outputPath, format, opts, nonPartitioned, experimentId)
      }

      var inputDF = if (nonPartitioned || roundNo != 0) {
        rawDF
      } else {
        rawDF.sort("partition","key")
      }

      val targetOpts = if (roundNo == 0) {
        opts
      } else {
        incrOpts
      }

      allRoundTimes += doWriteRound(
        inputDF,
        outputPath,
        format,
        apiType,
        saveMode,
        targetOperation,
        targetOpts,
        nonPartitioned,
        mergeConditionColumns,
        updateColumns,
        mergeMode,
        experimentId,
        roundNo,
        icebergSpjEnable)

      inputDF.unpersist()
    })

    println(s"""
         |$lineSepBold
         |Total time taken by all rounds (${format}): ${allRoundTimes.sum}
         |Per round: ${allRoundTimes.toList}
         |$lineSepBold
         |""".stripMargin)
  }

  def doWriteRound(
      inputDF: DataFrame,
      outputPath: String,
      format: StorageFormat = Parquet,
      apiType: ApiType = ApiType.SparkDatasourceApi,
      saveMode: SaveMode = SaveMode.Append,
      operation: OperationType = OperationType.Upsert,
      opts: Map[String, String] = Map(),
      nonPartitioned: Boolean = false,
      mergeConditionColumns: Seq[String],
      updateColumns: Seq[String],
      mergeMode: MergeMode,
      experimentId: String,
      roundNo: Int,
      icebergSpjEnable: Boolean): Long = {
    val startMs = System.currentTimeMillis()

    format match {
      case Hudi =>
        val tableName = genHudiTableName(experimentId)
        writeToHudi(
          inputDF,
          operation,
          outputPath,
          apiType,
          saveMode,
          opts,
          nonPartitioned,
          mergeConditionColumns,
          updateColumns,
          mergeMode,
          tableName)
      case Delta =>
        val tableName = s"delta-$experimentId"
        writeToDelta(
          inputDF,
          operation,
          outputPath,
          saveMode,
          nonPartitioned,
          mergeConditionColumns,
          updateColumns,
          mergeMode,
          tableName)
      case Parquet =>
        writeToParquet(inputDF, operation, outputPath, saveMode)
      case Iceberg =>
        val tableName = genIcebergTableName(experimentId)

        val icebergDF = if(roundNo > 0 && icebergSpjEnable && !nonPartitioned){
          enableStoragePartitionedJoin();
          createIcebergTableForSpj(inputDF.schema, outputPath, opts, nonPartitioned, experimentId, inputDF);
        }else{
          inputDF
        }
        writeToIceberg(
          icebergDF,
          operation,
          nonPartitioned,
          mergeConditionColumns,
          updateColumns,
          mergeMode,
          tableName)
      case _ =>
        throw new UnsupportedOperationException(s"$format is not supported")
    }

    val timeTaken = System.currentTimeMillis() - startMs
    println(s"Took ${timeTaken} ms.")
    timeTaken
  }

  private def writeToIceberg(
      df: DataFrame,
      operation: OperationType,
      nonPartitioned: Boolean,
      mergeConditionColumns: Seq[String],
      updateColumns: Seq[String],
      mergeMode: MergeMode,
      tableName: String): Unit = {
    val escapedTableName = escapeTableName(tableName)
    val repartitionedDF = if (nonPartitioned) {
      df
    } else {
      df.sort("partition","key")
    }
    repartitionedDF.createOrReplaceTempView(s"source")

    operation match {
      case OperationType.Insert =>
        // NOTE: Iceberg requires ordering of the dataset when being inserted into partitioned tables
        val insertIntoTableSql =
          s"""
             |INSERT INTO $escapedTableName
             |SELECT * FROM source ${if (nonPartitioned) "" else ""}
             |""".stripMargin

        executeSparkSql(spark, insertIntoTableSql)

      case OperationType.Upsert =>
        df.createOrReplaceTempView(s"source")

        val mergeCondition = buildMergeCondition(mergeConditionColumns)
        val updateSetClause = buildUpdateSetClause(updateColumns)

        val matchedClause = mergeMode match {
          case UpdateInsert =>
            s"WHEN MATCHED THEN UPDATE SET $updateSetClause"
          case DeleteInsert =>
            "WHEN MATCHED THEN DELETE"
        }

        // Execute MERGE INTO performing
        //   - Updates for all records w/ matching (partition, key) tuples
        //   - Inserts for all remaining records
        executeSparkSql(
          spark,
          s"""
             |MERGE INTO $escapedTableName t
             |USING (SELECT * FROM source s)
             |ON $mergeCondition
             |$matchedClause
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin)
    }
  }

  private def writeToDelta(
      df: DataFrame,
      operation: OperationType,
      outputPath: String,
      saveMode: SaveMode,
      nonPartitioned: Boolean,
      mergeConditionColumns: Seq[String],
      updateColumns: Seq[String],
      mergeMode: MergeMode,
      tableName: String): Unit = {
    val targetPath = s"$outputPath/$tableName"
    operation match {
      case OperationType.Insert =>
        val writer = df.write.format("delta")
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

          val mergeCondition =
            mergeConditionColumns.map(col => s"oldData.$col = newData.$col").mkString(" AND ")

          val mergeBuilder = deltaTable
            .as("oldData")
            .merge(df.as("newData"), mergeCondition)
          val matchedBuilder = mergeMode match {
            case UpdateInsert =>
              if (updateColumns.isEmpty) {
                mergeBuilder.whenMatched.updateAll()
              } else {
                val updateMap = buildUpdateExprMap(updateColumns)
                mergeBuilder.whenMatched.updateExpr(updateMap)
              }
            case DeleteInsert =>
              mergeBuilder.whenMatched.delete()
          }

          matchedBuilder.whenNotMatched
            .insertAll()
            .execute()
        }
    }
  }

  private def writeToParquet(
      df: DataFrame,
      operation: OperationType,
      outputPath: String,
      saveMode: SaveMode): Unit = {
    operation match {
      case OperationType.Insert =>
        df.write
          .format("parquet")
          .mode(saveMode)
          .save(s"$outputPath/parquet")

      case OperationType.Upsert =>
        throw new UnsupportedOperationException("Operation 'upsert' is not supported for Parquet")
    }
  }

  private def writeToHudi(
      df: DataFrame,
      operation: OperationType,
      outputPath: String,
      apiType: ApiType,
      saveMode: SaveMode,
      opts: Map[String, String],
      nonPartitioned: Boolean,
      mergeConditionColumns: Seq[String],
      updateColumns: Seq[String],
      mergeMode: MergeMode,
      tableName: String): Unit = {
    // TODO cleanup
    val repartitionedDF = if (nonPartitioned) {
      df
    } else {
      df.sort("partition","key")
    }
    repartitionedDF.createOrReplaceTempView("source")
    apiType match {
      case ApiType.SparkDatasourceApi =>
        require(
          mergeMode != MergeMode.DeleteInsert,
          "Hudi sparkDataSourceApi does not support delete operations.")
        require(
          updateColumns.isEmpty,
          "Hudi sparkDataSourceApi does not support partial column updates.")
        val partitionOpts = if (nonPartitioned) {
          Map.empty[String, String]
        } else {
          Map(DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition")
        }

        val targetOpts = opts ++ partitionOpts ++ Map(HoodieWriteConfig.TBL_NAME.key() -> "hudi")

        repartitionedDF.write
          .format("hudi")
          .options(targetOpts)
          .option(DataSourceWriteOptions.OPERATION.key, operation.asString)
          .mode(saveMode)
          .save(s"$outputPath/$tableName")

      case ApiType.SparkSqlApi =>
        repartitionedDF.createOrReplaceTempView("source")
        val escapedTableName = escapeTableName(tableName)

        operation match {
          case OperationType.Insert | OperationType.BulkInsert =>
            val insertIntoTableSql =
              s"""
                 |INSERT INTO $escapedTableName
                 |SELECT * FROM source
                 |""".stripMargin
            executeSparkSql(spark, insertIntoTableSql)
          case OperationType.Upsert =>
            val mergeCondition = buildMergeCondition(mergeConditionColumns)
            val updateSetClause = buildUpdateSetClause(updateColumns)
            val matchedClause = mergeMode match {
              case UpdateInsert =>
                s"WHEN MATCHED THEN UPDATE SET $updateSetClause"
              case DeleteInsert =>
                "WHEN MATCHED THEN DELETE"
            }

            // Execute MERGE INTO
            executeSparkSql(
              spark,
              s"""
                 |MERGE INTO $escapedTableName t
                 |USING (SELECT * FROM source s)
                 |ON $mergeCondition
                 |$matchedClause
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin)
        }
    }
  }

  private def escapeTableName(tableName: String) =
    tableName.split('.').map(np => s"`$np`").mkString(".")

  private def genIcebergTableName(experimentId: String): String =
    s"$catalog.$database.iceberg_$experimentId"

  private def genIcebergRawTableNameForSpj(experimentId: String): String =
    s"$catalog.$database.iceberg_${experimentId}_raw_spj"

  private def genHudiTableName(experimentId: String): String =
    s"$catalog.$database.hudi-$experimentId".replace("-", "_")
}

object IncrementalLoader {
  def main(args: Array[String]): Unit = {
    IncrementalLoaderParser.parser.parse(args, LoadConfig()) match {
      case Some(config) =>
        val format = StorageFormat.fromString(config.format)
        val apiType = ApiType.fromString(config.apiType)

        if (apiType == ApiType.SparkSqlApi && format != StorageFormat.Hudi) {
          System.err.println(s"Error: --api-type spark-sql is only supported with --format hudi. Got: --format ${config.format}")
          sys.exit(1)
        }

        val spark = SparkSession.builder
          .appName("lake-loader incremental data loader")
          .getOrCreate()

        val dataLoader =
          new IncrementalLoader(spark, config.numberOfRounds, config.catalog, config.database)

        val incrOptions = if (config.incrOptions.isEmpty) {
          config.options
        } else {
          config.incrOptions
        }

        dataLoader.doWrites(
          config.inputPath,
          config.outputPath,
          format = format,
          operation = OperationType.fromString(config.operationType),
          apiType = apiType,
          initialOperation = OperationType.fromString(config.initialOperationType),
          opts = config.options,
          incrOpts = incrOptions,
          nonPartitioned = config.nonPartitioned,
          experimentId = config.experimentId,
          startRound = config.startRound,
          mergeConditionColumns = IncrementalLoaderParser.getMergeConditionColumns(config),
          updateColumns = config.updateColumns,
          mergeMode = MergeMode.fromString(config.mergeMode),
          icebergSpjEnable = config.icebergSpjEnable)
        spark.stop()
      case None =>
        // scopt already prints help
        sys.exit(1)
    }
  }
}
