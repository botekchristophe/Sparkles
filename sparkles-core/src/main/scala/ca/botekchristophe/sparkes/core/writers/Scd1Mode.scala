/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */
package ca.botekchristophe.sparkes.core.writers

import ca.botekchristophe.sparkes.core.datasources.DataTable

trait Scd1Mode {self: DataTable =>
  val oidColumnName: String = s"${self.name}_oid"
  val createdOnColumnName: String = "created_on_dts"
  val updatedOnColumnName: String = "updated_on_dts"
}
