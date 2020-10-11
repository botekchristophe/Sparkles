/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core.datasources

trait FactTable extends DataTable {
  val eventDateColumnName: String = "event_dte"
}
