package org.apache.spark.sql.execution.streaming.sources.mssqlct

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

class ChangeTrackingProvider
  extends SimpleTableProvider with DataSourceRegister
    with Logging {
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
      jdbcOptions, primaryKeyColumns, otherColumns, suggestedMaxChanges, schema = null)

    new ChangeTrackingTable(tableInfo.copy(
      schema = Utils.getChangeTableSchema(tableInfo)
    ))
  }
}
