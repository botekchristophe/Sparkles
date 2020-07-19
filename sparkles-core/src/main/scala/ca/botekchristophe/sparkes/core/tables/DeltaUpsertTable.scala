/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.tables

import ca.botekchristophe.sparkes.core.datasources.{DataSource, DataTable, Format, Formats}
import ca.botekchristophe.sparkes.core.writers.UpsertMode

case class DeltaUpsertTable(override val name: String,
                            override val database: String,
                            override val dependencies: Set[DataSource]) extends DataTable with UpsertMode {
  override val format: Format = Formats.DELTA
}

