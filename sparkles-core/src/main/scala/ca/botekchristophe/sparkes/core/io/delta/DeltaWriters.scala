/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.io.delta

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import ca.botekchristophe.sparkes.core.file.FileSystem
import ca.botekchristophe.sparkes.core.tables.{DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

object DeltaWriters {

  implicit class DataFrameOps(df: DataFrame) {
    def getOrCreateColumn(columnName: String, value: Column): DataFrame = {
      df
        .columns.find(_.equals(columnName))
        .fold(
          df.withColumn(columnName, value)
        )(_=>
          df.withColumn(columnName, when(col(columnName).isNull, value).otherwise(col(columnName))))
    }
  }

  /**
   * Slow Changing Dimension type 1, implemented with delta.io api
   *
   * @param updates the data coming as an update
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return
   */
  def scd1(targetTable: DeltaScd1Table, updates: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
    val buid = targetTable.buidColumnName
    val oid = targetTable.oidColumnName
    val createdOn = targetTable.createdOnColumnName
    val updatedOn = targetTable.updatedOnColumnName
    val currentValue = Timestamp.valueOf(LocalDateTime.now())

    val updatesWithTechnicalFields =
      updates
        .getOrCreateColumn(createdOn, lit(currentValue))
        .getOrCreateColumn(updatedOn, lit(currentValue))

    Try {
      require(updates.columns.exists(_.equals(buid)), s"Scd1 requires column [$buid]")
      require(updates.columns.exists(_.equals(oid)), s"Scd1 requires column [$oid]")
    }.fold({error => Left(error.getMessage)}, {_ =>
      Try(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)))
        .fold({_ =>
          updatesWithTechnicalFields.write.format("delta").mode("overwrite").save(targetTable.location(fs))
        },{
          table =>
            val existingBuidOidPairs =
              table
                .toDF
                .select(col(buid), col(oid) as s"old_$oid")

            //Keep only the updates for which the rows was changed, ie. has a different oid
            val stagedUpdates =
              updatesWithTechnicalFields
                .join(existingBuidOidPairs, Seq(buid))
                .filter(col(s"old_$oid") =!= col(oid))
                .drop(s"old_$oid")

            //Merge the stagedUpdates to the existing table
            table
              .as("events")
              .merge(
                stagedUpdates.as("updates"),
                s"events.$buid = updates.$buid")
              .whenMatched
              .updateExpr(updatesWithTechnicalFields.columns.filterNot(_.equals(createdOn)).map(c => c -> s"updates.$c").toMap)
              .whenNotMatched
              .insertAll()
              .execute()
        })
      Right(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)).toDF)
    })
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
    val buid = targetTable.buidColumnName

    require(updates.columns.exists(_.equals(buid)), s"upsert requires column [$buid] to be defined")

    Try(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)))
      .toEither
      .fold({_ =>
        updates.write.format("delta").mode("overwrite").save(targetTable.location(fs))
      },{ table =>
        if (!table.toDF.isEmpty) {
          table
            .as("existing")
            .merge(
              updates.as("updates"),
              s"existing.$buid = updates.$buid")
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
    val uid = targetTable.uidColumnName
    val buid = targetTable.buidColumnName
    val oid = targetTable.oidColumnName
    val isCurrent = targetTable.isCurrentColumnName
    val validFrom = targetTable.validFromColumnName
    val validTo = targetTable.validToColumnName
    val currentValue = targetTable.infiniteValue match {
      case _: LocalDate => Date.valueOf(LocalDate.now())
      case _: LocalDateTime => Timestamp.valueOf(LocalDateTime.now())
      case _ => throw new IllegalArgumentException("Infinite value has to be of type either LocalDate or LocalDateTime")
    }
    val infiniteValue = targetTable.infiniteValue match {
      case ld: LocalDate => Date.valueOf(ld)
      case ldt: LocalDateTime => Timestamp.valueOf(ldt)
      case _ => throw new IllegalArgumentException("Infinite value has to be of type either LocalDate or LocalDateTime")
    }
    Try {
      require(updates.columns.exists(_.equals(buid)), s"Scd2 requires column [$buid] to be defined")
      require(updates.columns.exists(_.equals(oid)), s"Scd2 requires column [$oid] to be defined")
    }.fold({error => Left(error.getMessage)}, { _ =>
      val updatesWithTechnicalFields =
        updates
          .getOrCreateColumn(validFrom, lit(currentValue))
          .withColumn(validTo, lit(infiniteValue))
          .withColumn(uid, sha1(concat_ws("_", col(validFrom), col(buid))))
          .withColumn(isCurrent, lit(true))

      Try(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)))
        .toEither
        .fold({ _ =>
          updatesWithTechnicalFields.write.format("delta").mode("overwrite").save(targetTable.location(fs))
        }, { table =>

          val targetDF =
            table
              .toDF
              .select(col(buid), col(oid) as s"old_$oid")
              .where(isCurrent)

          val updatedRecord =
            updatesWithTechnicalFields
              .join(targetDF, Seq(buid))
              .where(col(oid) =!= col(s"old_$oid"))
              .drop(s"old_$oid")
              .withColumn("mergeKey", lit(null)) //this will ensure the new records are inserted

          val stagedUpdates =
            updatesWithTechnicalFields
              .withColumn("mergeKey", col(buid))
              .unionByName(updatedRecord)

          table
            .as("existing")
            .merge(
              stagedUpdates.as("updates"), s"existing.$buid = updates.mergeKey")
            .whenMatched(s"existing.$isCurrent = true AND existing.$oid <> updates.$oid")
            .updateExpr(Map(isCurrent -> "false", validTo -> s"updates.$validFrom"))
            .whenNotMatched()
            .insertExpr(
              updatesWithTechnicalFields
                .drop("mergeKey")
                .columns
                .map(c => c -> s"updates.$c").toMap
            ).execute()
        })
      Right(DeltaTable.forPath(SparkSession.active, targetTable.location(fs)).toDF)
    })
  }
}
