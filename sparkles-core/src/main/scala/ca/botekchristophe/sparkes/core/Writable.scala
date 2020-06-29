/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import cats.implicits._
import org.apache.spark.sql.DataFrame

import scala.util.Try

trait Writable[A] {
  def writeData(a: A, data: DataFrame): Either[String, DataFrame]
}

object Writable {

  implicit val deltaTableScd2CanInsert: Writable[DeltaScd2Table] =
    new Writable[DeltaScd2Table] {
      override def writeData(a: DeltaScd2Table, data: DataFrame): Either[String, DataFrame] = {
        Try(data.write.save(a.name)).toEither.map(_ => data).leftMap(_.getMessage)
      }
    }

  implicit class WritableImplicits[B](data: DataFrame) {
    def writeData(b: B)(implicit writable: Writable[B]): Either[String, DataFrame] = {
      writable.writeData(b, data)
    }
  }
}
