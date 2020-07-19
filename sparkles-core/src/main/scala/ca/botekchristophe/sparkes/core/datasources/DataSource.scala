/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

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
}
