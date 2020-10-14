/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core.datasources

/**
 * DimensionTable tables usually store informations that changes over time or can be updated.
 */
trait DimensionTable extends DataTable {
  val oidColumnName: String = s"${name}_oid"
}
