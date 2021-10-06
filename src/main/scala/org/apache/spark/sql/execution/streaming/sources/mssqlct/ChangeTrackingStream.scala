package org.apache.spark.sql.execution.streaming.sources.mssqlct

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.streaming.LongOffset
import scala.collection.mutable.{ArrayBuffer, Queue}

case class ChangeTrackingInputPartition(
    tableInfo: ChangeTrackingTableInfo,
    start: Long,
    end: Long) extends InputPartition

class ChangeTrackingStream(tableInfo: ChangeTrackingTableInfo)
    extends MicroBatchStream with Logging {

  private var committedOffset = LongOffset(0L)
  private var largestOffset = LongOffset(0L)
  private val nextVersions = new Queue[Long]()

  override def initialOffset(): Offset = synchronized {
    committedOffset
  }

  override def commit(end: Offset): Unit = synchronized {
    committedOffset = end.asInstanceOf[LongOffset]
    nextVersions.dropWhile(v => v <= committedOffset.offset)
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def latestOffset(): Offset = synchronized {
    nextVersions ++= Utils.getNextChanges(tableInfo, largestOffset.offset)
    if (nextVersions.length > 0) {
      largestOffset = LongOffset(nextVersions.max)
    }
    largestOffset
  }

  override def planInputPartitions(
      startOffset: Offset,
      endOffset: Offset): Array[InputPartition] = synchronized {
    val startVersion = startOffset.asInstanceOf[LongOffset].offset
    val endVersion = endOffset.asInstanceOf[LongOffset].offset
    nextVersions
      .filter(v => startVersion < v && v <= endVersion)
      .map(v => ChangeTrackingInputPartition(tableInfo, v-1, v).asInstanceOf[InputPartition])
      .toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    (partition: InputPartition) => {
      new ChangeTrackingReader(
        partition.asInstanceOf[ChangeTrackingInputPartition])
    }
  }

  override def stop(): Unit = synchronized {}

  override def toString: String = s"ChangeTrackingSource(${tableInfo.options.tableOrQuery})"
}
