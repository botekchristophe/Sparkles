/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

import ca.botekchristophe.sparkes.core.file.FileSystem

/**
 * A DataTable sitting on a data lake
 */
trait DataLakeTable extends DataTable with DataLakeFile {

  /**
   * Location of the source. This string can be passed to Spark in order to read the source
   */
  override def location(fs: FileSystem): String = {
    fs.getStorageRootPath(storageAlias).getOrElse(fs.defaultRootPath) + relativePath +
      domainName.fold(
        s"$sourceName/$name"
      )(domain =>
        s"$sourceName/$domain/$name"
      )
  }
}
