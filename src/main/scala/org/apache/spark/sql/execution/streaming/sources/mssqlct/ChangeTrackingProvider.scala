package org.apache.spark.sql.execution.streaming.sources.mssqlct

import java.util.{EnumSet, Set}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._

case class ChangeTrackingTableInfo(
  options: JDBCOptions,
  primaryKeyColumns: Array[String],
  otherColumns: Array[String],
  suggestedMaxChanges: Int,
  schema: StructType)

class ChangeTrackingProvider() extends SimpleTableProvider with DataSourceRegister with Logging {
  val PRIMARY_KEY_COLUMNS = "primaryKeyColumns"
  val SUGGESTED_MAX_CHANGES = "suggestedMaxChanges"

  override def shortName(): String = "mssqlct"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val jdbcOptions = new JDBCOptions(CaseInsensitiveMap(options.asScala.toMap))
    val primaryKeyColumns = options.getOrDefault(PRIMARY_KEY_COLUMNS, "").split(",")
    val suggestedMaxChanges = options.getOrDefault(SUGGESTED_MAX_CHANGES, "1000").toInt

    val dialect = JdbcDialects.get(jdbcOptions.url)
    val tableSchema = Utils.getQueryOutputSchema(
      dialect.getSchemaQuery(jdbcOptions.tableOrQuery), jdbcOptions)

    val otherColumns = tableSchema.fields
      .map(f => f.name)
      .filter(c => !primaryKeyColumns.contains(c))

    val tableInfo = new ChangeTrackingTableInfo(
      jdbcOptions, primaryKeyColumns, otherColumns, suggestedMaxChanges, /*schema*/null)

    new ChangeTrackingTable(tableInfo.copy(
      schema = Utils.getChangeTableSchema(tableInfo)
    ))
  }
}

class ChangeTrackingTable(tableInfo: ChangeTrackingTableInfo)
    extends Table with SupportsRead {

  override def name(): String = tableInfo.options.tableOrQuery

  override def schema(): StructType = tableInfo.schema

  override def capabilities(): Set[TableCapability] =
    EnumSet.of(TableCapability.MICRO_BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = tableInfo.schema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new ChangeTrackingStream(tableInfo)
  }
}
