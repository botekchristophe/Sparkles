/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

import ca.botekchristophe.sparkes.core.io.UpsertMode

/**
 * A trait for fact tables with updates.
 */
trait UpsertTable extends FactTable with UpsertMode
