/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core

import org.apache.spark.sql.DataFrame

/**
 * A DataSource can be read by Spark as a DataFrame
 */
trait DataSource {

  /**
   * Defines a standard way of reading this type of Source
   * @param location can be a file location or a table location in a bdms engine
   * @return either an error message or a DataFrame
   */
  def read(location: String*): Either[String, DataFrame]
}
