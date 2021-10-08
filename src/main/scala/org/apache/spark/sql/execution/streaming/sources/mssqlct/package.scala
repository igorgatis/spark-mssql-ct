package org.apache.spark.sql.execution.streaming.sources.mssqlct

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType

case class ChangeTrackingTableInfo(
                                    options: JDBCOptions,
                                    primaryKeyColumns: Array[String],
                                    otherColumns: Array[String],
                                    suggestedMaxChanges: Int,
                                    schema: StructType)

case class ChangeTrackingInputPartition(
                                         tableInfo: ChangeTrackingTableInfo,
                                         start: Long,
                                         end: Long) extends InputPartition
