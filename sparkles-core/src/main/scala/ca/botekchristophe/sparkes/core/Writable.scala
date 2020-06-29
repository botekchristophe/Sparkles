/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writable[A] {
  def write(a: A, data: DataFrame): Unit
}

object Writable {

  implicit val deltaTableScd2CanInsert: Writable[DeltaScd2Table] =
    new Writable[DeltaScd2Table] {
      override def write(a: DeltaScd2Table, data: DataFrame): Unit = data.write.save(a.name)
    }

  implicit class WritableImplicits[B](data: DataFrame) {
    def write(b: B)(implicit writable: Writable[B]): Unit = {
      writable.write(b, data)
    }
  }
}

