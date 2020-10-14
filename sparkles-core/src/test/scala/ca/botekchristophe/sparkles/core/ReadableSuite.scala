/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core

import ca.botekchristophe.sparkes.core.Readable._
import ca.botekchristophe.sparkes.core.file.LocalFileSystem
import ca.botekchristophe.sparkes.core.tables.DeltaScd2Table
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class ReadableSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  val df: DataFrame = List(("a", 1)).toDF("col1", "col2")
  val location: String = getClass.getClassLoader.getResource(".").getPath


  //TODO fix this
  "Readable" should "read delta scd2 table" in {
    LocalFileSystem.list(location).foreach(_.foreach(f => println(f.path)))
    //println(location)

    spark.readData(DeltaScd2Table("path/relative/", "source", "name", "database")).isLeft shouldBe true
  }
}
