/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core.writers

import ca.botekchristophe.sparkes.core.datasources.DataTable

trait UpsertMode {self: DataTable =>
  val eventDateColumnName: String = "event_dte"
}
