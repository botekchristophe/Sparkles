/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core.io

import java.time.LocalDate

import ca.botekchristophe.sparkes.core.datasources.DimensionTable

trait Scd2Mode {self: DimensionTable =>

  val validFromColumnName: String = "valid_from_dte"

  val validToColumnName: String = "valid_to_dte"

  val infiniteValue: Any = LocalDate.of(9999, 12, 31)

  val isCurrentColumnName: String = "is_current_flag"
}
