/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import ca.botekchristophe.sparkes.core.tables.{DeltaInsertTable, DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import cats.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

trait Readable[A] {
  def readData(a: A,
               from: LocalDateTime = LocalDateTime.MIN,
               to: LocalDateTime = LocalDateTime.MAX): Either[String, DataFrame]
}

object Readable {

  implicit val deltaTableScd2CanBeRead: Readable[DeltaScd2Table] = new Readable[DeltaScd2Table] {
    override def readData(a: DeltaScd2Table, from: LocalDateTime, to: LocalDateTime): Either[String, DataFrame] = {
      Try(SparkSession.active.read.format("delta").load(a.name))
        .toEither
        .leftMap(_.getMessage)
        .map(_.filter(col(a.validToColumnName) <= Timestamp.valueOf(to) and col(a.validToColumnName) >= Timestamp.valueOf(to)))
    }
  }

  implicit val deltaTableScd1CanBeRead: Readable[DeltaScd1Table] = new Readable[DeltaScd1Table] {
    override def readData(a: DeltaScd1Table, from: LocalDateTime, to: LocalDateTime): Either[String, DataFrame] = {
      Try(SparkSession.active.read.format("delta").load(a.name))
        .toEither
        .leftMap(_.getMessage)
    }
  }

  implicit val deltaTableInsertCanBeRead: Readable[DeltaInsertTable] = new Readable[DeltaInsertTable] {
    override def readData(a: DeltaInsertTable, from: LocalDateTime, to: LocalDateTime): Either[String, DataFrame] = {

      Try(SparkSession.active.read.format("delta").load(a.name))
        .toEither
        .leftMap(_.getMessage)
        .map(_.filter(col(a.eventDateColumnName).between(Date.valueOf(from.toLocalDate), Date.valueOf(to.toLocalDate))))
    }
  }

  implicit val deltaTableUpsertCanBeRead: Readable[DeltaUpsertTable] = new Readable[DeltaUpsertTable] {
    override def readData(a: DeltaUpsertTable, from: LocalDateTime, to: LocalDateTime): Either[String, DataFrame] = {
      Try(SparkSession.active.read.format("delta").load(a.name))
        .toEither
        .leftMap(_.getMessage)
        .map(_.filter(col(a.eventDateColumnName).between(Date.valueOf(from.toLocalDate), Date.valueOf(to.toLocalDate))))
    }
  }

  implicit class ReadableImplicits[B](spark: SparkSession) {
    def readData(b: B,
                 from: LocalDateTime = LocalDateTime.MIN,
                 to: LocalDateTime = LocalDateTime.MAX)(implicit read: Readable[B]): Either[String, DataFrame] = {
      read.readData(b, from, to)
    }
  }
}
