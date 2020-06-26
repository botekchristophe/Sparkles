/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core

import java.util.UUID

import ca.botekchristophe.sparkes.core.DeltaSource
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class DeltaSourceSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val df: DataFrame = List(("a", 1)).toDF("col1", "col2")
  val location: String = getClass.getClassLoader.getResource(".") + "DeltaSourceSuite"

  "A DeltaSource read" should "succeed when location is correct" in {
    df.write.format("delta").mode(SaveMode.Overwrite).save(location)
    DeltaSource.read(location).isRight shouldBe true
  }

  "A DeltaSource read" should "fail when arguments are empty" in {
    DeltaSource.read().isLeft shouldBe true
  }

  "A DeltaSource read" should "fail when arguments are 2 or more" in {
    DeltaSource.read(location, location).isLeft shouldBe true
  }

  "A DeltaSource read" should "fail when location does not exist" in {
    DeltaSource.read(UUID.randomUUID().toString).isLeft shouldBe true
  }
}
