/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

/**
 * A DataTable is any table sitting on a DataLake, a DBMS or a NoSQL system.
 */
trait DataTable extends DataSource {

  /**
   * A DataTable can have a database name associated
   */
  val database: String = "default"

  /**
   * A buid or business identifier, represents the row in term of business value.
   * It can be unique in the whole table or not but doesn't HAVE to be unique.
   * In general, it is a HASH of all the columns representing the business keys
   */
  val buidColumnName: String = s"${name}_buid"

  /**
   * A uid or unique identifier, has to be unique in the table. it is used to access the data.
   * In some cases the buid and the uid can hold the same value.
   * In general, it is a HASH of all the columns representing the primary keys
   */
  val uidColumnName: String = s"${name}_uid"
}
