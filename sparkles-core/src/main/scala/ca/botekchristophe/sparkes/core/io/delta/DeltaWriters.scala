/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.io.delta

import ca.botekchristophe.sparkes.core.file.FileSystem
import ca.botekchristophe.sparkes.core.tables.{DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import cats.implicits._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object DeltaWriters {

  /**
   * Slow Changing Dimension type 1, implemented with delta.io api
   *
   * @param updates the data coming as an update
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return
   */
  def scd1(targetTable: DeltaScd1Table, updates: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
    Try(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)))
      .toEither
      .fold({_ =>
        updates.write.format("delta").mode("overwrite").save(targetTable.location(fs))
      },{
        table =>
          if (!table.toDF.isEmpty) {
            table
              .as("events")
              .merge(
                updates.as("updates"),
                s"events.${targetTable.buidColumnName} = updates.${targetTable.buidColumnName}")
              .whenMatched
              .updateExpr(updates.columns.filterNot(_.equals(targetTable.createdOnColumnName)).map(c => c -> s"updates.$c").toMap)
              .whenNotMatched
              .insertAll()
              .execute()

          } else {
            updates.write.format("delta").mode("overwrite").save(targetTable.location(fs))
          }
      })
    Right(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)).toDF)
  }

  /**
   * Upsert, implemented with delta.io api
   *
   * @param updates the data coming as an update
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return
   */
  def upsert(targetTable: DeltaUpsertTable, updates: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
    Try(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)))
      .toEither
      .fold({_ =>
        updates.write.format("delta").mode("overwrite").save(targetTable.location(fs))
      },{
        table =>
          if (!table.toDF.isEmpty) {
            table
              .as("events")
              .merge(
                updates.as("updates"),
                s"events.${targetTable.buidColumnName} = updates.${targetTable.buidColumnName}")
              .whenMatched
              .updateAll()
              .whenNotMatched
              .insertAll()
              .execute()
          } else {
            updates.write.format("delta").mode("overwrite").save(targetTable.location(fs))
          }
      })
    Right(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)).toDF)
  }
  /**
   * Slow Changing Dimension Type2, implemented with delta.io api
   *
   * @param updates the data coming as an update
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return
   */
  def scd2(targetTable: DeltaScd2Table, updates: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
    Try(updates.write.format(targetTable.format.sparkFormat).mode(SaveMode.Overwrite).save(targetTable.location(fs)))
      .toEither.map(_ => updates)
      .leftMap(_.getMessage)
  }
}