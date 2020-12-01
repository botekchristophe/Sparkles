/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core.io

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID

import ca.botekchristophe.sparkes.core.file.{FileSystem, LocalFileSystem}
import ca.botekchristophe.sparkes.core.io.Readable._
import ca.botekchristophe.sparkes.core.io.Writable._
import ca.botekchristophe.sparkes.core.tables.{DeltaInsertTable, DeltaScd1Table, DeltaScd2Table, DeltaUpsertTable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class WritableSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val day1 = Timestamp.valueOf(LocalDateTime.of(1900, 1, 1, 0, 0 ,0))
  val day2 = Timestamp.valueOf(LocalDateTime.of(1900, 1, 2, 0, 0 ,0))

  val initialLoad: DataFrame = List(
    ("a", "a", day1, day1),
    ("b", "b", day1, day1)
  ).toDF("table_buid", "table_oid", "created_on_dts", "updated_on_dts")

  val updateLoad: DataFrame = List(
    ("a", "aa", day2, day2),
    ("b", "b" , day2, day2)
  ).toDF("table_buid", "table_oid", "created_on_dts", "updated_on_dts")

  val fs: FileSystem = LocalFileSystem


  "Writable" should "write delta scd2 table" in {

    val testTable = DeltaScd2Table("path/relative/", UUID.randomUUID().toString, "table", "database")

    initialLoad.writeData(testTable, fs = fs).isRight shouldBe true
    spark.readData(testTable, fs = fs).right.get.show(false)
    updateLoad.writeData(testTable, fs = fs).isRight shouldBe true
    spark.readData(testTable, fs = fs).right.get.show(false)
  }

  "Writable" should "write delta scd1 table" in {

    val testTable = DeltaScd1Table("path/relative/", UUID.randomUUID().toString, "table", "database")

    initialLoad.writeData(testTable, fs = fs).isRight shouldBe true
    val resultInitialLoad = spark.readData(testTable, fs = fs).right.get
    resultInitialLoad.show(false)
    resultInitialLoad
      .where($"table_buid".isin("a") and $"created_on_dts".isin(day1) and $"updated_on_dts".isin(day1))
      .count() shouldBe 1L

    resultInitialLoad
      .where($"table_buid".isin("b") and $"created_on_dts".isin(day1) and $"updated_on_dts".isin(day1))
      .count() shouldBe 1L

    updateLoad.writeData(testTable, fs = fs).isRight shouldBe true
    val resultSecondLoad = spark.readData(testTable, fs = fs).right.get
    resultSecondLoad
      .where($"table_buid".isin("a") and $"created_on_dts".isin(day1) and $"updated_on_dts".isin(day2))
      .count() shouldBe 1L

    resultSecondLoad
      .where($"table_buid".isin("b") and $"created_on_dts".isin(day1) and $"updated_on_dts".isin(day1))
      .count() shouldBe 1L
  }

  "Writable" should "write delta upsert table" in {

    val testTable = DeltaUpsertTable("path/relative/", UUID.randomUUID().toString, "table", "database")

    initialLoad.writeData(testTable, fs = fs).isRight shouldBe true
    val resultInitialLoad = spark.readData(testTable, fs = fs).right.get
    resultInitialLoad
      .where($"table_buid".isin("a") and $"created_on_dts".isin(day1) and $"updated_on_dts".isin(day1))
      .count() shouldBe 1L

    resultInitialLoad
      .where($"table_buid".isin("b") and $"created_on_dts".isin(day1) and $"updated_on_dts".isin(day1))
      .count() shouldBe 1L

    updateLoad.writeData(testTable, fs = fs).isRight shouldBe true
    val resultSecondLoad = spark.readData(testTable, fs = fs).right.get
    resultSecondLoad
      .where($"table_buid".isin("a") and $"created_on_dts".isin(day2) and $"updated_on_dts".isin(day2))
      .count() shouldBe 1L

    resultSecondLoad
      .where($"table_buid".isin("b") and $"created_on_dts".isin(day2) and $"updated_on_dts".isin(day2))
      .count() shouldBe 1L
  }

  "Writable" should "write delta insert table" in {

    val testTable = DeltaInsertTable("path/relative/", UUID.randomUUID().toString, "table", "database")

    initialLoad.writeData(testTable, fs = fs).isRight shouldBe true
    val resultInitialLoad = spark.readData(testTable, fs = fs).right.get
    resultInitialLoad.count() shouldBe 2L
    updateLoad.writeData(testTable, fs = fs).isRight shouldBe true
    val resultSecondLoad = spark.readData(testTable, fs = fs).right.get
    resultSecondLoad.count() shouldBe 4L
  }
}
