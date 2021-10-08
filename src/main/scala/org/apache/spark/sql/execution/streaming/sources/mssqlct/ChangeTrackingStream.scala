package org.apache.spark.sql.execution.streaming.sources.mssqlct

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.streaming.LongOffset

import scala.collection.mutable

class ChangeTrackingStream(tableInfo: ChangeTrackingTableInfo)
  extends MicroBatchStream with Logging {

  private var committedOffset = LongOffset(0L)
  private var largestOffset = LongOffset(0L)
  private val nextVersions = new mutable.Queue[Long]()

  override def initialOffset(): Offset = synchronized {
    logError(s"initialOffset=$committedOffset")
    committedOffset
  }

  override def commit(end: Offset): Unit = synchronized {
    logError(s"commit($end)")
    committedOffset = end.asInstanceOf[LongOffset]
    nextVersions.dropWhile(v => v <= committedOffset.offset)
  }

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def latestOffset(): Offset = synchronized {
    try {
      nextVersions ++= Utils.getNextChanges(tableInfo, largestOffset.offset)
      if (nextVersions.nonEmpty) {
        largestOffset = LongOffset(nextVersions.max)
      }
      logError(s"largestOffset=$largestOffset")
    } catch {
      case e: Exception => logError(s"$e")
    }
    largestOffset
  }

  override def planInputPartitions(
                                    startOffset: Offset,
                                    endOffset: Offset): Array[InputPartition] = synchronized {
    val start = startOffset.asInstanceOf[LongOffset].offset
    val end = endOffset.asInstanceOf[LongOffset].offset
    val filteredVersions = nextVersions
      .filter(v => start < v && v <= end)
    logError(s"planInputPartitions($start, $end): $filteredVersions")
    filteredVersions
      .map(v => ChangeTrackingInputPartition(tableInfo, v - 1, v).asInstanceOf[InputPartition])
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
