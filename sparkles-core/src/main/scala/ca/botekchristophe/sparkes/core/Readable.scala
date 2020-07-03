/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import java.time.LocalDateTime

import cats.implicits._
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
      Try(SparkSession.active.read.format("delta").load(a.name)).toEither.leftMap(_.getMessage)
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
