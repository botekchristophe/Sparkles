/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

import ca.botekchristophe.sparkes.core.file.FileSystem
import org.apache.spark.sql.types.StructType

/**
 * A DataSource is the base definition of any Data sources, this could be a stream, a file, a set of files
 * or even a table in a DBMS.
 */
trait DataSource {
  /**
   * A name to represent the source. Ideally you want to make this unique per storage, queue or database.
   */
  val name: String

  /**
   * In order to be processed in a batch or real-time, DataSource depend between each other.
   * It is important to set a general rule as per which the graph of dependency needs to remain
   * Acyclic and Directed.
   */
  val dependencies: Set[DataSource]

  /**
   * The format of the DataSource, see [[Format]] for examples.
   */
  val format: Format

  /**
   * In general, you want to enforce the schema on your tables in Gold zone,
   * But there can be a case for loose schema in bronze and silver zones and thus keeping
   * the schema of a DataSource optional
   */
  val schema: Option[StructType] = None

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
   * Location of the source. This string can be passed to Spark in order to read the source
   */
  def location(fs: FileSystem): String
}
