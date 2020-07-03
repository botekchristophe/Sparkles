/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core

import ca.botekchristophe.sparkes.core.DeltaScd2Table
import ca.botekchristophe.sparkes.core.Readable._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class ReadableSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  val df: DataFrame = List(("a", 1)).toDF("col1", "col2")
  val location: String = getClass.getClassLoader.getResource(".") + "DeltaSourceSuite"


  "Readable" should "read delta scd2 table" in {
    spark.readData(DeltaScd2Table("", "", Set())).isLeft shouldBe true
  }
}
