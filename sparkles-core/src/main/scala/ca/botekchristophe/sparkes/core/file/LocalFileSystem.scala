/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.file

import cats.implicits._
import org.apache.commons.lang.NotImplementedException

import scala.annotation.tailrec
import scala.util.Try

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
   * If the param 'recursive' is set to true, the function must copy all files contained in the subtree
   * If the param 'recursive' is false and the file is a directory the function must fail.
   * @param source source file path
   * @param destination destination file path
   * @param recursive recursive flag
   * @return returns Unit if the copy was successful
   *         returns an error if the copy failed
   */
  override def copy(source: String, destination: String, recursive: Boolean): Either[String, Unit] = {
    throw new NotImplementedException("Method Not implemented")
  }

  /**
   * Removes file(s)
   * @param path path to the file(s) to remove
   * @return returns Unit if the removal was successful
   *         returns an error if the removal failed
   */
  override def remove(path: String): Either[String, Unit] = {
    throw new NotImplementedException("Method Not implemented")
  }
}
