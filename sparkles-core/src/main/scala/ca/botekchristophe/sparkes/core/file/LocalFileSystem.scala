/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.file

import java.io
import java.io.IOException

import cats.implicits._
import org.apache.commons.io.FileUtils

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Implementation of [[FileSystem]] for local development or testing.
 */
object LocalFileSystem extends FileSystem {

  /**
   * Based on a storageAlias, returns the root path of that storage.
   *
   * @param storageAlias storage alias
   * @return returns an error if the storage alias doesn't exist.
   *         returns the storage root path if the alias exists.
   */
  override def getStorageRootPath(storageAlias: String): Either[String, String] = {
    storageAlias match {
      case ""   => Left("Invalid storage name")
      case name => Right(getClass.getClassLoader.getResource(".").getPath + s"$name/")
    }
  }

  /**
   * PRIVATE - implementation of a recursive list file for the filesystem
   * @param root root where to start the lookup. Usually a directory.
   * @return a list of all files in the root directory along with the files contained in sub directories.
   *         DO NOT includes sub directories
   */
  private def tailRecursiveListFiles(root: File): List[File] = {
    @tailrec
    def loop(fileDone: List[java.io.File], fileTodo: List[java.io.File]): List[java.io.File] = {
      fileTodo match {
        case head :: tail if !head.isDirectory => loop(fileDone ++ List(head), tail)
        case head :: tail if head.isDirectory => loop(fileDone, tail ++ head.listFiles().toList)
        case Nil => fileDone
      }
    }
    loop(List(), new java.io.File(root.path).listFiles().toList)
      .map(file => File(file.getAbsolutePath, file.getName, file.length(), file.isDirectory))
  }

  /**
   * List the files in a directory, can be recursive or non recursive.
   * @param path path of the directory where to start the lookup
   * @param recursive if true, the lookup will be recursive.
   * @return the list of all files in the root directory along with the files in the sub directories if parameter
   *         'recursive' is true.
   *         Returns an error if anything went wrong during the listing.
   */
  override def list(path: String, recursive: Boolean = true): Either[String, List[File]] = {
    Try(
      if(recursive) {
        tailRecursiveListFiles(File(path, "", 0L, isDir = false))
      } else {
        new java.io.File(path)
          .listFiles()
          .toList
          .map(file => File(file.getAbsolutePath, file.getName, file.length(), file.isDirectory))
      }
    )
      .toEither
      .leftMap(_.getMessage)
  }

  /**
   * Copy a file to a destination folder.
   *
   * @param source source file path
   * @param destination destination file path
   * @param overwrite flag
   * @return returns Unit if the copy was successful
   *         returns an error if the copy failed
   */
  override def copy(source: String, destination: String, overwrite: Boolean = false): Either[String, Unit] = {
    Try {
      val src = new io.File(source)
      val dst = new io.File(destination)
      if (dst.exists() && !overwrite) {
        throw new IOException(s"Destination non empty. Remove file(s) or use overwrite argument.")
      }
      if (src.isDirectory) {
        FileUtils.copyDirectory(src, dst)
      } else {
        FileUtils.copyFile(src, dst)
      }
    } match {
      case Failure(e) => Left(e.getLocalizedMessage)
      case Success(_) => Right()
    }
  }

  /**
   * Removes file(s)
   * @param path path to the file(s) to remove
   * @return returns Unit if the removal was successful
   *         returns an error if the removal failed
   */
  override def remove(path: String): Either[String, Unit] = {
    Try {
      FileUtils.deleteQuietly(new io.File(path))
    } match {
      case Failure(e) => Left(e.getLocalizedMessage)
      case Success(_) => Right()
    }
  }
}
