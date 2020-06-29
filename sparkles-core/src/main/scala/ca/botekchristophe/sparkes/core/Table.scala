/**
 * Copyright (C) 2020-2020 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

trait Table {
  val database: String
  val name: String
  val partitionBy: Option[String] = None
}

trait Tracable {self: Table =>
  val ingestionFileColumnName: String = "IngestionFileNn"
  val ingestionDateTimeColumnName: String = "IngestionDts"
}

trait InsertWrite {self: Table =>
  val eventDateColumnName: String = "EventDte"
}

trait UpsertWrite {self: Table =>
  val eventDateColumnName: String = "EventDte"
}

trait Scd1Write {self: Table =>
  val buidColumnName: String = s"${self.name}_buid"
  val oidColumnName: String = s"${self.name}_oid"
  val createdOnColumnName: String = "CreatedOnDts"
  val updatedOnColumnName: String = "UpdatedOnDts"
}

trait Scd2Write {self: Table =>
  val uidColumnName: String = s"${self.name}_uid"
  val buidColumnName: String = s"${self.name}_buid"
  val oidColumnName: String = s"${self.name}_oid"
  val validFromColumName: String = "ValidFromDte"
  val validToColumName: String = "ValidFromDte"
}
case class DeltaScd2Table(override val name: String,
                          override val database: String) extends Table with Scd2Write
case class DeltaScd1Table(override val name: String,
                          override val database: String) extends Table with Scd1Write
