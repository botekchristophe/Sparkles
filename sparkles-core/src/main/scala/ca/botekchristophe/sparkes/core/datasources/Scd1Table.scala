/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

import ca.botekchristophe.sparkes.core.writers.Scd1Mode

/**
 * A trait for Slow changing dimension type 1 tables.
 */
trait Scd1Table extends DimensionTable with Scd1Mode
