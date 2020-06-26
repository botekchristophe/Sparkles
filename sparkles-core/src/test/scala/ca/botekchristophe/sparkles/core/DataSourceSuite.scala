/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core

import ca.botekchristophe.sparkes.core.DataSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class DataSourceSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  "A DeltaSource read" should "fail when arguments are empty" in {
    new DataSource {
      /**
       * Defines a standard way of reading this type of Source
       *
       * @param location can be a file location or a table location in a bdms engine
       * @return either an error message or a DataFrame
       */
      override def read(location: String*): Either[String, DataFrame] = Right(spark.emptyDataFrame)
    }
  }
}
