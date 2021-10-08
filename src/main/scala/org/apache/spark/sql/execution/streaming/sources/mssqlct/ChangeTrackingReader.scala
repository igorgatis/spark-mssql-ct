package org.apache.spark.sql.execution.streaming.sources.mssqlct

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

import java.sql.{Connection, PreparedStatement}

class ChangeTrackingReader(
                            partition: ChangeTrackingInputPartition
                          ) extends PartitionReader[InternalRow] with Logging {

  private var initialized: Boolean = false
  private var connection: Connection = null
  private var statement: PreparedStatement = null
  private var rows: Iterator[InternalRow] = null

  private def initIfNeeded(): Unit = {
    if (initialized) return
    initialized = true

    val tableInfo = partition.tableInfo
    val query = Utils.buildQueryForData(tableInfo, partition.start, partition.end)
    try {
      connection = JdbcUtils.createConnectionFactory(tableInfo.options)()
      statement = connection.prepareStatement(query)
      statement.setQueryTimeout(tableInfo.options.queryTimeout)
      rows = Utils.resultSetToSparkInternalRows(statement.executeQuery(), tableInfo.schema)
    } catch {
      case e: Exception => {
        logError(s"$e")
        close()
        throw e
      }
    }
  }

  override def next(): Boolean = {
    initIfNeeded()
    rows != null && rows.hasNext
  }

  override def get(): InternalRow = {
    initIfNeeded()
    if (rows != null && rows.hasNext) {
      rows.next
    } else {
      null
    }
  }

  override def close(): Unit = {
    try {
      if (statement != null) {
        statement.close()
        statement = null
      }
    } finally {
      if (connection != null) {
        connection.close()
        connection = null
      }
    }
  }
}
