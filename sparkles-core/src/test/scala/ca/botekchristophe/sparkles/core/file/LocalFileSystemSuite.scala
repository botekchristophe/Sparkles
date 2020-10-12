/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core.file

import java.io.File

import ca.botekchristophe.sparkes.core.file.LocalFileSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class LocalFileSystemSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val location: String = getClass.getClassLoader.getResource(".").getPath

  "LocalFileSystem" should "list files in class loader" in {
    //val files = LocalFileSystem.list(location)

    LocalFileSystem.list(location).foreach(println)

    //files.isLeft shouldBe false
  }
}
