/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.catalog

import ca.botekchristophe.sparkes.core.datasources.DataSource

trait Catalog {

  val dataSources: Set[DataSource]

  def getImpactedDataSources: DataSource => Set[DataSource]
}
