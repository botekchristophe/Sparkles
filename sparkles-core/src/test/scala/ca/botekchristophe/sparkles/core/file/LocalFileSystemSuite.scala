/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkles.core.file

import java.util.UUID

import ca.botekchristophe.sparkes.core.file.{File, LocalFileSystem}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers

class LocalFileSystemSuite extends AnyFlatSpec with matchers.should.Matchers {

  val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  "LocalFileSystem" should "succeed when passed a valid storage name" in {
    LocalFileSystem.getStorageRootPath("default").isRight shouldBe true
  }

  "LocalFileSystem" should "fail when passed an empty string" in {
    LocalFileSystem.getStorageRootPath("").isLeft shouldBe true
  }

  "LocalFileSystem" should "list files in default storage" in {
    val random = UUID.randomUUID().toString.replace("-", "")

    Seq((1), (2)).toDF("a").coalesce(1).write.format("csv").save(LocalFileSystem.defaultRootPath + random)

    val result = LocalFileSystem.list(LocalFileSystem.defaultRootPath)

    result.isRight shouldBe true
    val files = result.getOrElse(List.empty[File])
    
    //print files for debug
    files.foreach(f => println(f.path))
    files.count(_.name.endsWith(".csv")) shouldBe 1

  }
}
