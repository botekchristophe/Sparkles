/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core
import cats.implicits._
import io.delta.tables._
import org.apache.spark.sql.DataFrame

import scala.util.Try

object DeltaSource extends DataSource {

  /**
   * Defines a standard way of reading this type of Source
   *
   * @param location can be a file location or a table location in a bdms engine
   * @return a DataFrame
   */
  override def read(location: String*): Either[String, DataFrame] = {
    if (location.size == 1) {
      Try(DeltaTable.forPath(location.head).toDF).toEither.leftMap(_.getMessage)
    } else {
      Left("Illegal value for 'location' in DeltaSource.read()")
    }
  }
}
