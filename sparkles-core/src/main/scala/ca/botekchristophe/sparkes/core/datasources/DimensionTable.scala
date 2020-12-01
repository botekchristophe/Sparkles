/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core.datasources

/**
 * DimensionTable tables usually store information that changes over time or can be updated.
 */
trait DimensionTable extends DataTable {

  /**
   * The oid, or object id, represents the hash of all fields that can have updates over time.
   * It improves performances when comparing two records with multiple columns.
   */
  val oidColumnName: String = s"${name}_oid"
}
