/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

/**
 * For audit or debugging purposes it can be useful to trace the data coming in a out
 * of the data lake, and thus to store some information about the origin of the data along with the
 * data itself.
 */
trait Tracable {self: DataTable =>
  /**
   * Column holding the full path of the originating file
   */
  val ingestionFileColumnName: String = "IngestionFileNn"

  /**
   * Column holding the timestamp of the ingestion
   */
  val ingestionDateTimeColumnName: String = "IngestionDts"
}
