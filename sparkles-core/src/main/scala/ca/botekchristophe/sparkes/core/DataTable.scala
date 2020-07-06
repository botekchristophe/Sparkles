/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core

import ca.botekchristophe.sparkes.core.datasource.{Format, Formats}

trait DataSource {
  val name: String
  val dependencies: Set[DataSource]
  val format: Format
}

trait DataTable extends DataSource {
  val database: String
  val partitionByColumnName: Option[String] = None
}

trait DataStream extends DataSource {
  val topic: String
}

trait DatalakeFile {
  val storageAlias: String
  val relativePath: String
  val location: String
}

trait Tracable {self: DataTable =>
  val ingestionFileColumnName: String = "IngestionFileNn"
  val ingestionDateTimeColumnName: String = "IngestionDts"
}

trait InsertWrite {self: DataTable =>
  val eventDateColumnName: String = "EventDte"
}

trait UpsertWrite {self: DataTable =>
  val eventDateColumnName: String = "EventDte"
}

trait Scd1Write {self: DataTable =>
  val buidColumnName: String = s"${self.name}_buid"
  val oidColumnName: String = s"${self.name}_oid"
  val createdOnColumnName: String = "CreatedOnDts"
  val updatedOnColumnName: String = "UpdatedOnDts"
}

trait Scd2Write {self: DataTable =>
  val uidColumnName: String = s"${self.name}_uid"
  val buidColumnName: String = s"${self.name}_buid"
  val oidColumnName: String = s"${self.name}_oid"
  val validFromColumName: String = "ValidFromDte"
  val validToColumName: String = "ValidFromDte"
}
case class DeltaScd2Table(override val name: String,
                          override val database: String,
                          override val dependencies: Set[DataSource]) extends DataTable with Scd2Write {
  override val format: Format = Formats.DELTA
}
case class DeltaScd1Table(override val name: String,
                          override val database: String,
                          override val dependencies: Set[DataSource]) extends DataTable with Scd1Write {
  override val format: Format = Formats.DELTA
}
