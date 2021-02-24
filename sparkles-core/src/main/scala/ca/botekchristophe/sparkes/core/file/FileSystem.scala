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

  /**
   * Calls getStorageRootPath with "default" as parameter.
   * @return default root path
   */
  def defaultRootPath: String = {
    getStorageRootPath("default")
      .getOrElse(throw new Exception("getStorageRootPath method was called with 'default' as a parameter but failed." +
        "It is recommended to implement a default behavior for this method."))
  }

  /**
   * Based on a storageAlias, returns the root path of that storage.
   *
   * @param storageAlias storage alias
   * @return returns an error if the storage alias doesn't exist.
   *         returns the storage root path if the alias exists.
   */
  def getStorageRootPath(storageAlias: String): Either[String, String]

  /**
   * List the files in a directory, can be recursive or non recursive.
   * @param path path of the directory where to start the lookup
   * @param recursive if true, the lookup will be recursive.
   * @return the list of all files in the root directory along with the files in the sub directories if parameter
   *         'recursive' is true.
   *         Returns an error if anything went wrong during the listing.
   */
  def list(path: String, recursive: Boolean): Either[String, List[File]]

  /**
   * Copy a file to a destination folder.
   *
   * @param source source file path
   * @param destination destination file path
   * @param overwrite flag
   * @return returns Unit if the copy was successful
   *         returns an error if the copy failed
   */
  def copy(source: String, destination: String, overwrite: Boolean): Either[String, Unit]

  /**
   * move a file to a destination folder.
   *
   * @param source source file path
   * @param destination destination file path
   * @param overwrite flag
   * @return returns Unit if the copy was successful
   *         returns an error if the move failed
   */
  def move(source: String, destination: String, overwrite: Boolean): Either[String, Unit]

  /**
   * Removes file(s)
   * @param path path to the file(s) to remove
   * @return returns Unit if the removal was successful
   *         returns an error if the removal failed
   */
  def remove(path: String): Either[String, Unit]
}
