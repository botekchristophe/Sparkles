/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import java.time.LocalDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Readable[A] {
  def read(a: A,
           from: LocalDateTime = LocalDateTime.MIN,
           to: LocalDateTime = LocalDateTime.MAX): DataFrame
}

object Readable {

  implicit val deltaTableScd2CanBeRead: Readable[DeltaScd2Table] = new Readable[DeltaScd2Table] {
    override def read(a: DeltaScd2Table, from: LocalDateTime, to: LocalDateTime): DataFrame = {
      SparkSession.active.read.format("delta").load(a.name)
    }
  }

  implicit class ReadableImplicits[B](spark: SparkSession) {
    def read(b: B,
             from: LocalDateTime = LocalDateTime.MIN,
             to: LocalDateTime = LocalDateTime.MAX)(implicit read: Readable[B]): DataFrame = {
      read.read(b, from, to)
    }
  }
}
