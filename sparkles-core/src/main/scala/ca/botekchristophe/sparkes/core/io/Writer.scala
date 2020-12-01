/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.io

import ca.botekchristophe.sparkes.core.datasources.{InsertTable, Scd1Table, Scd2Table, UpsertTable}
import ca.botekchristophe.sparkes.core.file.FileSystem
import org.apache.spark.sql.DataFrame

/**
 * Trait defining writer implementation for different data storage systems.
 */
trait Writer {

  /**
   * Insert only
   *
   * @param updates the incoming data to be inserted
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return the entire data in the target table
   */
  def insert(targetTable: InsertTable, updates: DataFrame, fs: FileSystem): Either[String, DataFrame]

  /**
   * Slow Changing Dimension type 1
   *
   * @param updates the data coming as an update
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return the entire data in the target table
   */
  def scd1(targetTable: Scd1Table, updates: DataFrame, fs: FileSystem): Either[String, DataFrame]

  /**
   * Insert / Update write mode
   *
   * @param updates the incoming data to be inserted or updated
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return the entire data in the target table
   */
  def upsert(targetTable: UpsertTable, updates: DataFrame, fs: FileSystem): Either[String, DataFrame]

  /**
   * Slow Changing Dimension Type2, implemented with delta.io api
   *
   * @param updates the data coming as an update
   * @param targetTable the configuration of the target table
   * @param fs an instance of [[FileSystem]]
   * @return the entire data in the target table
   */
  def scd2(targetTable: Scd2Table, updates: DataFrame, fs: FileSystem): Either[String, DataFrame]
}
