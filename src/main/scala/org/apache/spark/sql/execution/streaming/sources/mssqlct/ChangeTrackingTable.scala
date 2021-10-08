package org.apache.spark.sql.execution.streaming.sources.mssqlct

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class ChangeTrackingTable(tableInfo: ChangeTrackingTableInfo)
  extends Table with SupportsRead {

  override def name(): String = tableInfo.options.tableOrQuery

  override def schema(): StructType = tableInfo.schema

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = tableInfo.schema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new ChangeTrackingStream(tableInfo)
  }
}
