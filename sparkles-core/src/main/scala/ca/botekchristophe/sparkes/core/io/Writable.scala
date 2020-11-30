/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.io

import ca.botekchristophe.sparkes.core.datasources.DataSource
import ca.botekchristophe.sparkes.core.file.FileSystem
import ca.botekchristophe.sparkes.core.io.delta.DeltaWriters
import ca.botekchristophe.sparkes.core.tables.{DeltaInsertTable, DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import cats.implicits._
import org.apache.spark.sql.DataFrame

import scala.util.Try

trait Writable[A] {
  def writeData(a: A, data: DataFrame, fs: FileSystem): Either[String, DataFrame]
}

object Writable {

  def genericWriteSource(a: DataSource, data: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
    Try(data.write.format(a.format.sparkFormat).options(a.writeOptions).save(a.location(fs)))
      .toEither.map(_ => data)
      .leftMap(_.getMessage)
  }

  implicit val deltaTableScd2CanWrite: Writable[DeltaScd2Table] =
    new Writable[DeltaScd2Table] {
      override def writeData(a: DeltaScd2Table, data: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
        DeltaWriters.scd2(a, data, fs)
      }
    }

  implicit val deltaTableScd1CanWrite: Writable[DeltaScd1Table] =
    new Writable[DeltaScd1Table] {
      override def writeData(a: DeltaScd1Table, data: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
        DeltaWriters.scd1(a, data, fs)
      }
    }

  implicit val deltaTableUpsertCanWrite: Writable[DeltaUpsertTable] =
    new Writable[DeltaUpsertTable] {
      override def writeData(a: DeltaUpsertTable, data: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
        DeltaWriters.upsert(a, data, fs)
      }
    }

  implicit val deltaTableInsertCanWrite: Writable[DeltaInsertTable] =
    new Writable[DeltaInsertTable] {
      override def writeData(a: DeltaInsertTable, data: DataFrame, fs: FileSystem): Either[String, DataFrame] = {
        genericWriteSource(a, data, fs)
      }
    }

  implicit class WritableImplicits[B](data: DataFrame) {
    def writeData(b: B, fs: FileSystem)(implicit writable: Writable[B]): Either[String, DataFrame] = {
      writable.writeData(b, data, fs)
    }
  }
}

