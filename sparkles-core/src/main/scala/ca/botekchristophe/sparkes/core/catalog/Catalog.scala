/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.catalog

import ca.botekchristophe.sparkes.core.datasources.DataSource

/**
 * A data Catalog can be used to store configurations about data sources.
 */
trait Catalog {

  /**
   * The list of data sources in the catalog
   */
  val dataSources: Set[DataSource]
}
