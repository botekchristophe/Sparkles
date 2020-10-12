/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.file

import cats.implicits._
import org.apache.commons.lang.NotImplementedException

import scala.util.Try

object LocalFileSystem extends FileSystem {

  //TODO make it tail recursive
  private def recursiveListFiles(root: File): List[File] = {
    new java.io.File(root.path)
      .listFiles()
      .toList
      .flatMap(file =>
        if(file.isDirectory) {
          recursiveListFiles(File(file.getAbsolutePath, file.getName, file.length(), file.isDirectory))
        } else {
          List(File(file.getAbsolutePath, file.getName, file.length(), file.isDirectory))
        }
      )
  }

  override def list(path: String, recursive: Boolean): Either[String, List[File]] = {
    Try(
      if(recursive) {
        recursiveListFiles(File(path, "", 0L, isDir = false))
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

  override def copy(source: String, destination: String, recursive: Boolean): Either[String, Unit] = {
    throw new NotImplementedException("Method Not implemented")
  }

  override def remove(path: String): Either[String, Unit] = {
    throw new NotImplementedException("Method Not implemented")
  }

}
