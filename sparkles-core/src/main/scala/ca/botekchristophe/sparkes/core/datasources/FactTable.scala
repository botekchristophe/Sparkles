/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core.datasources

/**
 * Fact tables usually store events or facts that do not change over time and are tied to a date.
 */
trait FactTable extends DataTable {
  val eventDateColumnName: String = "event_dte"
}
