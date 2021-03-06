/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

import ca.botekchristophe.sparkes.core.io.Scd2Mode

/**
 * A trait for Slow changing dimension type 2 tables.
 */
trait Scd2Table extends DimensionTable with Scd2Mode
