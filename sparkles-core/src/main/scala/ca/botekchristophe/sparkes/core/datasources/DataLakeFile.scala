/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

/**
 * A DataLakeFile represents a file or many files containing structured or unstructured data.
 * When the files can only be represented as a table, use [[ca.botekchristophe.sparkes.core.DataTable]] instead.
 * In certain cases, like with Delta Lake table we might want to mix DataTable and DataLakeFile traits together.
 * Usually, this trait is used for raw file directly deposited on a Data Lake.
 */
trait DataLakeFile extends DataSource {

  /**
   * In case you have multiple storage, you can use this to define a name for the storage.
   */
  val storageAlias: String = "default"

  /**
   * Relative path of the file starting from the storage root all the way to the sourceName.
   * In general this can be Bronze/, Silver/ or Gold/ but you can also include sub divisions that
   * fit your organization but that does not fit with the data source.
   */
  val relativePath: String

  /**
   * Name of the source or the system where the data is coming from.
   */
  val sourceName: String

  /**
   * OPTIONAL FIELD
   * The system source might be organized with sub domain.
   * For instance, divided in different languages or different line of businesses.
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
   * When the data is to big to be sitting in one folder, it can be necessary to make partitions.
   * The general rule of thumb is to create partition when each of them can hold more than a GigaByte of data.
   */
  val partitionBy: Option[String] = None

  /**
   * Regex matching all the files from the same data source.
   * This can be used to filter the files when doing a listFile() inside a folder.
   */
  val fileMatchRegex: String =
    domainName.fold(
      s"$sourceName/\\d{4}/\\d{2}/\\d{2}/${name}_\\d{8}_\\d{6}${format.fileExtension}"
    )(domain =>
      s"$sourceName/$domain/\\d{4}/\\d{2}/\\d{2}/${name}_\\d{8}_\\d{6}${format.fileExtension}"
    )

  /**
   * Location of the file(s). This string can be passed to Spark in order to read the file(s)
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
