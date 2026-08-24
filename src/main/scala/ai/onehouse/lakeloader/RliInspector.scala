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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, length}

/**
 * One-off diagnostic: reads RLI (record_index) content via the hudi_metadata() SQL table-valued
 * function for one or more Hudi table base paths, and prints average key length / partition-name
 * length per table. Used to compare RLI record shape across tables when shard file sizes differ
 * despite identical record counts.
 */
object RliInspector {
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Usage: RliInspector <tablePath> [<tablePath> ...]")

    val spark = SparkSession.builder
      .appName("lake-loader RLI inspector")
      .getOrCreate()

    val sep = "=" * 100
    println(sep)

    args.foreach { tablePath =>
      val riDf = spark.sql(s"select key, recordIndexMetadata.partitionName as partitionName " +
        s"from hudi_metadata('$tablePath') where type = 5")
      riDf.cache()
      val count = riDf.count()
      val stats = riDf
        .select(
          avg(length(riDf("key"))).as("avgKeyLen"),
          avg(length(riDf("partitionName"))).as("avgPartitionNameLen"))
        .collect()(0)
      println(s"$tablePath:")
      println(s"  RLI entries: $count")
      println(s"  avg key length: ${stats.getAs[Double]("avgKeyLen")}")
      println(s"  avg partitionName length: ${stats.getAs[Double]("avgPartitionNameLen")}")
      riDf.unpersist()
    }

    println(sep)
    spark.stop()
  }
}
