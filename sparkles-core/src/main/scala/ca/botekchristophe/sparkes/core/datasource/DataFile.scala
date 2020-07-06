/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasource

import ca.botekchristophe.sparkes.core.DataSource

/**
 * A DataFile represent a file or many files containing structured or unstructured data.
 * When the files can be represented as a table, use [[ca.botekchristophe.sparkes.core.DataTable]] instead.
 * Usually, this trait is used for raw file directly deposited on a Data Lake.
 */
trait DataFile extends DataSource {

  /**
   * Name of the source or system where the data is coming from.
   */
  val sourceName: String

  /**
   * OPTIONAL FIELD
   * The system source might be organized with sub domain.
   * For instance, different language or line of business.
   * If you do not wish to use a domain, you can leave the value empty
   */
  val domainName: Option[String]

  /**
   * For some files like JSON or CSV, we might want to set specific readOptions to Spark.
   * Can be empty
   */
  val readOptions: Map[String, String]

  /**
   * We might want to set specific writeOptions to Spark when writing raw files.
   * Can be empty
   */
  val writeOptions: Map[String, String]

  /**
   * Name of the folder to use for new files
   */
  val landingFolder: String = "Landing"

  /**
   * Name of the folder to use when the files are processed
   */
  val archiveFolder: String = "Archive"

  /**
   * Partitioning pattern for the data file.
   * Supports all formats in  [[java.text.SimpleDateFormat]]
   */
  val partitionPattern: String =
    domainName.fold(
      s"$sourceName/yyyy/MM/dd/${name}_yyyyMMdd_HHmmss${format.fileExtension}"
    )(domain =>
      s"$sourceName/$domain/yyyy/MM/dd/${name}_yyyyMMdd_HHmmss${format.fileExtension}"
    )

  /**
   * Regex used to filter the files when doing a listFiles in a folder.
   */
  val fileMatchRegex: String =
    domainName.fold(
      s"$sourceName/\\d{4}/\\d{2}/\\d{2}/${name}_\\d{8}_\\d{6}${format.fileExtension}"
    )(domain =>
      s"$sourceName/$domain/\\d{4}/\\d{2}/\\d{2}/${name}_\\d{8}_\\d{6}${format.fileExtension}"
    )

  /**
   * Location of the file(s). This string can be passed to Spark in order to
   */
  val location: String =
    domainName.fold(
      s"$sourceName/*/*/*/${name}_*_*${format.fileExtension}"
    )(domain =>
      s"$sourceName/$domain/*/*/*/${name}_*_*${format.fileExtension}"
    )

  /**
   * Regex extracting the file timestamp.
   */
  val fileTimestampExtractRegex: String = s"${name}_(yyyyMMdd_HHmmss)${format.fileExtension}"
}
