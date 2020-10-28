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

  val random: String = UUID.randomUUID().toString.replace("-", "")

  Seq(1, 2)
    .toDF("a")
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .save(LocalFileSystem.defaultRootPath + random)

  "LocalFileSystem" should "succeed when passed a valid storage name" in {
    LocalFileSystem.getStorageRootPath("default").isRight shouldBe true
  }

  "LocalFileSystem" should "fail when passed an empty string" in {
    LocalFileSystem.getStorageRootPath("").isLeft shouldBe true
  }

  "LocalFileSystem" should "list files in default storage" in {
    val result = LocalFileSystem.list(LocalFileSystem.defaultRootPath + random)

    result.isRight shouldBe true
    result.getOrElse(List.empty[File]).count(_.name.endsWith(".csv")) shouldBe 1
  }

  "LocalFileSystem" should "copy files in default storage" in {
    val source = LocalFileSystem.defaultRootPath + random
    val destination = LocalFileSystem.defaultRootPath + random + "_copy"

    val result1 = LocalFileSystem.list(destination)

    result1.getOrElse(List()).count(_.name.endsWith(".csv")) shouldBe 0

    LocalFileSystem.copy(source, destination)
    val result2 = LocalFileSystem.list(destination)

    result2.getOrElse(List.empty[File]).count(_.name.endsWith(".csv")) shouldBe 1

    //second time should fail
    LocalFileSystem.copy(source, destination).isLeft shouldBe true

    //third time should overwrite
    LocalFileSystem.copy(source, destination, overwrite = true).isRight shouldBe true
  }

  "LocalFileSystem" should "remove files in default storage" in {
    val source = LocalFileSystem.defaultRootPath + random
    val destination = LocalFileSystem.defaultRootPath + random + "_delete"

    val result1 = LocalFileSystem.list(destination)

    result1.getOrElse(List()).count(_.name.endsWith(".csv")) shouldBe 0

    LocalFileSystem.copy(source, destination)
    LocalFileSystem.remove(destination)
    val result2 = LocalFileSystem.list(destination)
    result2.getOrElse(List.empty[File]).count(_.name.endsWith(".csv")) shouldBe 0
  }
}
