/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.file

/**
 * Representation of the file system of a datalake.
 * It defines common ways of listing, moving, deleting files
 */
trait FileSystem {

  def list(path: String, recursive: Boolean = true): Either[String, List[File]]

  def copy(source: String, destination: String, recursive: Boolean = true): Either[String, Unit]

  def remove(path: String): Either[String, Unit]
}
