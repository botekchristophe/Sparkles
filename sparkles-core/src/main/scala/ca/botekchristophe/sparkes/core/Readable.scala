/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import ca.botekchristophe.sparkes.core.datasources.{DataSource, FactTable, Scd2Table}
import ca.botekchristophe.sparkes.core.file.FileSystem
import ca.botekchristophe.sparkes.core.tables.{DeltaInsertTable, DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import cats.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

trait Readable[A] {
  /**
   * A way of reading data and returning a Dataframe as a result.
   * the data can be filtered using a time range defined as two LocalDateTimes.
   * The lower and upper bound of the range must be included in the result.
   *
   * @param a a type A that is [[Readable]].
   * @param from an optional lower bound of the time range.
   * @param to an optional upper bound of the time range.
   * @return a Dataframe containing the data filtered if any of the time range is defined. Else returns all the data.
   */
  def readData(a: A,
               from: Option[LocalDateTime] = None,
               to: Option[LocalDateTime] = None,
               fs: FileSystem): Either[String, DataFrame]
}

object Readable {

  def genericReadDataSource(a: DataSource, fs: FileSystem): Either[String, DataFrame] = {
    Try(SparkSession.active.read.options(a.readOptions).format(a.format.sparkFormat).load(a.location(fs)))
      .toEither
      .leftMap(_.getMessage)
  }

  def genericReadScd2Table(a: Scd2Table, to: Option[LocalDateTime], fs: FileSystem): Either[String, DataFrame] = {
    val result: Either[String, DataFrame] = genericReadDataSource(a, fs)
    to.fold {
      result
    }{datetime =>
      result
        .map(_.filter(col(a.validFromColumnName) <= Timestamp.valueOf(datetime) and col(a.validToColumnName) >= Timestamp.valueOf(datetime)))
    }
  }

  def genericReadFactTable(a: FactTable, from: Option[LocalDateTime], to: Option[LocalDateTime], fs: FileSystem): Either[String, DataFrame] = {
    val data = genericReadDataSource(a, fs)
    (from, to) match {
      case (Some(lowerLimit), Some(upperLimit)) =>
        data.map(_.filter(col(a.eventDateColumnName).between(Date.valueOf(lowerLimit.toLocalDate), Date.valueOf(upperLimit.toLocalDate))))

      case (None, Some(upperLimit)) =>
        data.map(_.filter(col(a.eventDateColumnName) <= Date.valueOf(upperLimit.toLocalDate)))

      case (Some(lowerLimit), None) =>
        data.map(_.filter(col(a.eventDateColumnName) >= Date.valueOf(lowerLimit.toLocalDate)))

      case (None, None) => data
    }
  }

  implicit val deltaTableScd2CanBeRead: Readable[DeltaScd2Table] = new Readable[DeltaScd2Table] {
    override def readData(a: DeltaScd2Table, from: Option[LocalDateTime], to: Option[LocalDateTime], fs: FileSystem): Either[String, DataFrame] = {
      genericReadScd2Table(a, to, fs)
    }
  }

  implicit val deltaTableScd1CanBeRead: Readable[DeltaScd1Table] = new Readable[DeltaScd1Table] {
    override def readData(a: DeltaScd1Table, from: Option[LocalDateTime], to: Option[LocalDateTime], fs: FileSystem): Either[String, DataFrame] = {
      genericReadDataSource(a, fs)
    }
  }

  implicit val deltaTableInsertCanBeRead: Readable[DeltaInsertTable] = new Readable[DeltaInsertTable] {
    override def readData(a: DeltaInsertTable, from: Option[LocalDateTime], to: Option[LocalDateTime], fs: FileSystem): Either[String, DataFrame] = {
      genericReadFactTable(a, from, to, fs)
    }
  }

  implicit val deltaTableUpsertCanBeRead: Readable[DeltaUpsertTable] = new Readable[DeltaUpsertTable] {
    override def readData(a: DeltaUpsertTable, from: Option[LocalDateTime], to: Option[LocalDateTime], fs: FileSystem): Either[String, DataFrame] = {
      genericReadFactTable(a, from, to, fs)
    }
  }

  implicit class ReadableImplicits[B](spark: SparkSession) {
    def readData(b: B,
                 from: Option[LocalDateTime] = None,
                 to: Option[LocalDateTime] = None,
                 fs: FileSystem)(implicit read: Readable[B]): Either[String, DataFrame] = {
      read.readData(b, from, to, fs)
    }
  }
}
