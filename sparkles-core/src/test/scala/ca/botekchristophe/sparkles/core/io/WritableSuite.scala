/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core.io

import java.util.UUID

import ca.botekchristophe.sparkes.core.io.Writable._
import ca.botekchristophe.sparkes.core.file.{FileSystem, LocalFileSystem}
import ca.botekchristophe.sparkes.core.tables.{DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class WritableSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  val initialLoad: DataFrame = List(
    ("a", 1)
  ).toDF("table_buid", "value")

  val updateLoad: DataFrame = List(
    ("a", 2)
  ).toDF("table_buid", "value")

  val fs: FileSystem = LocalFileSystem


  "Writable" should "write delta scd2 table" in {

    val testTable = DeltaScd2Table("path/relative/", UUID.randomUUID().toString, "table", "database")

    initialLoad.writeData(testTable, fs = fs).isRight shouldBe true
    updateLoad.writeData(testTable, fs = fs).isRight shouldBe true
  }

  "Writable" should "write delta scd1 table" in {

    val testTable = DeltaScd1Table("path/relative/", UUID.randomUUID().toString, "table", "database")

    initialLoad.writeData(testTable, fs = fs).isRight shouldBe true
    updateLoad.writeData(testTable, fs = fs).isRight shouldBe true
  }

  "Writable" should "write delta upsert table" in {

    val testTable = DeltaUpsertTable("path/relative/", UUID.randomUUID().toString, "table", "database")

    initialLoad.writeData(testTable, fs = fs).isRight shouldBe true
    updateLoad.writeData(testTable, fs = fs).isRight shouldBe true
  }
}
