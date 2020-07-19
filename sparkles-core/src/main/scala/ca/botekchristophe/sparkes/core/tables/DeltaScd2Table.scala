/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.tables

import ca.botekchristophe.sparkes.core.datasources._
import ca.botekchristophe.sparkes.core.writers.Scd2Mode
import org.apache.spark.sql.types.StructType

case class DeltaScd2Table(override val relativePath: String,
                          override val sourceName: String,
                          override val domainName: Option[String],
                          override val name: String,
                          override val database: String,
                          override val schema: Option[StructType],
                          override val dependencies: Set[DataSource])

  extends DataTable with Scd2Mode with DataLakeFile {

  /**
   * For some files like JSON or CSV, we might want to set specific readOptions to Spark.
   * Can be empty
   */
  override val readOptions: Map[String, String] = Map.empty[String, String]
  /**
   * We might want to set specific writeOptions to Spark when writing raw files.
   * Can be empty
   */
  override val writeOptions: Map[String, String] = Map.empty[String, String]

  /**
   * The format of the DataSource, see [[Format]] for examples.
   */
  override val format: Format = Formats.DELTA
}

object DeltaScd2Table {

  def apply(relativePath: String,
            sourceName: String,
            domainName: String,
            name: String,
            database: String,
            schema: Option[StructType],
            dependencies: Set[DataSource]): DeltaInsertTable =
    new DeltaInsertTable(relativePath, sourceName, Some(domainName), name, database, schema, dependencies)

  def apply(relativePath: String,
            sourceName: String,
            name: String,
            database: String,
            schema: Option[StructType],
            dependencies: Set[DataSource]): DeltaInsertTable =
    new DeltaInsertTable(relativePath, sourceName, None, name, database, schema, dependencies)
}

