/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import ca.botekchristophe.sparkes.core.datasources.DataSource
import ca.botekchristophe.sparkes.core.tables.{DeltaInsertTable, DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import cats.implicits._
import org.apache.spark.sql.DataFrame

import scala.util.Try

trait Writable[A] {
  def writeData(a: A, data: DataFrame): Either[String, DataFrame]
}

object Writable {

  def genericWriteSource(a: DataSource, data: DataFrame): Either[String, DataFrame] = {
    Try(data.write.format(a.format.sparkFormat).options(a.writeOptions).save(a.location))
      .toEither.map(_ => data)
      .leftMap(_.getMessage)
  }

  implicit val deltaTableScd2CanInsert: Writable[DeltaScd2Table] =
    new Writable[DeltaScd2Table] {
      override def writeData(a: DeltaScd2Table, data: DataFrame): Either[String, DataFrame] = {
        genericWriteSource(a, data)
      }
    }

  implicit val deltaTableScd1CanInsert: Writable[DeltaScd1Table] =
    new Writable[DeltaScd1Table] {
      override def writeData(a: DeltaScd1Table, data: DataFrame): Either[String, DataFrame] = {
        genericWriteSource(a, data)
      }
    }

  implicit val deltaTableUpsertCanInsert: Writable[DeltaUpsertTable] =
    new Writable[DeltaUpsertTable] {
      override def writeData(a: DeltaUpsertTable, data: DataFrame): Either[String, DataFrame] = {
        genericWriteSource(a, data)
      }
    }

  implicit val deltaTableInsertCanInsert: Writable[DeltaInsertTable] =
    new Writable[DeltaInsertTable] {
      override def writeData(a: DeltaInsertTable, data: DataFrame): Either[String, DataFrame] = {
        genericWriteSource(a, data)
      }
    }

  implicit class WritableImplicits[B](data: DataFrame) {
    def writeData(b: B)(implicit writable: Writable[B]): Either[String, DataFrame] = {
      writable.writeData(b, data)
    }
  }
}

