/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.file

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

class HadoopFileSystem(bucket: String) extends FileSystem {
  /**
   * Based on a storageAlias, returns the root path of that storage.
   *
   * @param storageAlias storage alias
   * @return returns an error if the storage alias doesn't exist.
   *         returns the storage root path if the alias exists.
   */
  override def getStorageRootPath(storageAlias: String): Either[String, String] = {
    Right(s"$bucket/$storageAlias/")
  }

  /**
   * List the files in a directory, can be recursive or non recursive.
   *
   * @param path      path of the directory where to start the lookup
   * @param recursive if true, the lookup will be recursive.
   * @return the list of all files in the root directory along with the files in the sub directories if parameter
   *         'recursive' is true.
   *         Returns an error if anything went wrong during the listing.
   */
  override def list(path: String, recursive: Boolean): Either[String, List[File]] = {
    Try {
      val folderPath = new Path(path)
      val conf = SparkSession.active.sparkContext.hadoopConfiguration
      val fs = folderPath.getFileSystem(conf)
      val it = fs.listFiles(folderPath, true)
      var files: List[File] = List()
      while(it.hasNext) {
        val item = it.next()
        files :+ File(item.getPath.toString, item.getPath.getName, item.getLen ,item.isDirectory)
      }
      files
    } match {
      case Failure(exception) => Left(exception.getMessage)
      case Success(value) => Right(value)
    }
  }

  /**
   * Copy a file to a destination folder.
   *
   * @param source      source file path
   * @param destination destination file path
   * @param overwrite   flag
   * @return returns Unit if the copy was successful
   *         returns an error if the copy failed
   */
override def copy(source:  String, destination:  String, overwrite: Boolean): Either[String, Unit] = ???

  /**
   * Removes file(s)
   *
   * @param path path to the file(s) to remove
   * @return returns Unit if the removal was successful
   *         returns an error if the removal failed
   */
override def remove(path: String): Either[String, Unit] = ???
}
