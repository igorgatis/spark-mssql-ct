package org.apache.spark.sql.execution.streaming.sources.mssqlct

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator

import java.sql.{Connection, ResultSet, SQLException}
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

object Utils extends Logging {
  private def buildChangesQuery(
                                 tableInfo: ChangeTrackingTableInfo,
                                 version: Long = 0,
                                 where: String = ""): String = {

    val tableName = tableInfo.options.tableOrQuery

    val cols = ArrayBuffer(
      s"ct.SYS_CHANGE_VERSION",
      s"ct.SYS_CHANGE_OPERATION"
    )
    tableInfo.primaryKeyColumns
      .foreach(c => cols += s"ct.[$c]")
    tableInfo.otherColumns
      .foreach(c => cols += s"t.[$c]")

    val joinPkExp = tableInfo.primaryKeyColumns
      .map(c => s"(ct.[$c] = t.[$c])")
      .mkString(" AND ")

    var versionExpr = if (version >= 0) s"$version" else "NULL"

    val lines = Array(
      s"SELECT " + cols.mkString(", "),
      s"FROM CHANGETABLE(CHANGES [$tableName], $versionExpr) AS ct",
      s"LEFT OUTER JOIN [$tableName] t ON $joinPkExp",
      if (where.nonEmpty) s"WHERE ($where)" else "",
      s"ORDER BY ct.SYS_CHANGE_VERSION",
      ";"
    )

    lines.filter(l => l.nonEmpty).mkString(" ")
  }

  def getChangeTableSchema(tableInfo: ChangeTrackingTableInfo): StructType = {
    val query = buildChangesQuery(tableInfo, where = "(1=0)")
    getQueryOutputSchema(query, tableInfo.options)
  }

  def buildQueryForData(
                         tableInfo: ChangeTrackingTableInfo,
                         startVersion: Long,
                         endVersion: Long): String = {
    buildChangesQuery(
      tableInfo,
      version = startVersion,
      where = s"(ct.SYS_CHANGE_VERSION <= $endVersion)")
  }

  def getNextChanges(tableInfo: ChangeTrackingTableInfo, lastVersion: Long): Array[Long] = {
    val tableName = tableInfo.options.tableOrQuery
    val query = if (tableInfo.suggestedMaxChanges > 0) {
      s"""
         |SELECT DISTINCT(ct2.SYS_CHANGE_VERSION) FROM (
         |  SELECT TOP(${tableInfo.suggestedMaxChanges}) ct.SYS_CHANGE_VERSION
         |  FROM CHANGETABLE(CHANGES [$tableName], $lastVersion) AS ct
         |  ORDER BY ct.SYS_CHANGE_VERSION
         |) AS ct2
      """.stripMargin
    } else {
      s"""
         |SELECT DISTINCT(ct.SYS_CHANGE_VERSION)
         |FROM CHANGETABLE(CHANGES [$tableName], $lastVersion) AS ct
      """.stripMargin
    }
    var result = ArrayBuffer[Long]()
    executeQuery(query, tableInfo.options, (rs: ResultSet) => {
      while (rs.next()) {
        result += rs.getLong(1)
      }
    })
    result.toArray
  }

  def getMinValidVersion(tableInfo: ChangeTrackingTableInfo): Option[Long] = {
    val tableName = tableInfo.options.tableOrQuery
    val query = s"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('$tableName'));"
    readLongScalar(query, tableInfo.options)
  }

  def getCurrentVersion(tableInfo: ChangeTrackingTableInfo): Option[Long] = {
    val query = s"SELECT CHANGE_TRACKING_CURRENT_VERSION();"
    readLongScalar(query, tableInfo.options)
  }

  def executeQuery[TResult](
                             query: String,
                             options: JDBCOptions,
                             action: (ResultSet) => TResult): TResult = {
    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
    try {
      val statement = conn.prepareStatement(query)
      try {
        statement.setQueryTimeout(options.queryTimeout)
        val rs = statement.executeQuery()
        try {
          action(rs)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  def readLongScalar(query: String, jdbcOptions: JDBCOptions): Option[Long] = {
    executeQuery(query, jdbcOptions, (rs: ResultSet) =>
      rs.next() match {
        case true => Some(rs.getLong(1))
        case false => None
      })
  }

  def getQueryOutputSchema(query: String, jdbcOptions: JDBCOptions): StructType = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val action = (rs: ResultSet) => JdbcUtils.getSchema(rs, dialect, alwaysNullable = true)
    executeQuery(query, jdbcOptions, action)
  }

  // scalastyle:off
  // Copy from:
  // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala
  // scalastyle:on

  def resultSetToSparkInternalRows(
                                    resultSet: ResultSet,
                                    schema: StructType): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      private[this] val rs = resultSet
      private[this] val getters: Array[JDBCValueGetter] = makeGetters(schema)
      private[this] val mutableRow = new SpecificInternalRow(schema.fields.map(x => x.dataType))

      override protected def close(): Unit = {
        try {
          rs.close()
        } catch {
          case e: Exception => logWarning("Exception closing resultset", e)
        }
      }

      override protected def getNext(): InternalRow = {
        if (rs.next()) {
          var i = 0
          while (i < getters.length) {
            getters(i).apply(rs, mutableRow, i)
            if (rs.wasNull) mutableRow.setNullAt(i)
            i = i + 1
          }
          mutableRow
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }
    }
  }

  // A `JDBCValueGetter` is responsible for getting a value from `ResultSet` into a field
  // for `MutableRow`. The last argument `Int` means the index for the value to be set in
  // the row and also used for the value in `ResultSet`.
  private type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit

  /**
   * Creates `JDBCValueGetter`s according to [[StructType]], which can set
   * each value from `ResultSet` to each field of [[InternalRow]] correctly.
   */
  private def makeGetters(schema: StructType): Array[JDBCValueGetter] =
    schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))

  private def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
        } else {
          row.update(pos, null)
        }

    // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
    // object returned by ResultSet.getBigDecimal is not correctly matched to the table
    // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
    // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
    // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
    // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
    // retrieve it, you will get wrong result 199.99.
    // So it is needed to set precision and scale for Decimal based on JDBC metadata.
    case DecimalType.Fixed(p, s) =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val decimal =
          nullSafeConvert[java.math.BigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        var ans = 0L
        var j = 0
        while (j < bytes.length) {
          ans = 256 * ans + (255 & bytes(j))
          j = j + 1
        }
        row.setLong(pos, ans)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setShort(pos, rs.getShort(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setByte(pos, rs.getByte(pos + 1))

    case StringType if metadata.contains("rowid") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, UTF8String.fromString(rs.getRowId(pos + 1).toString))

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    // SPARK-34357 - sql TIME type represents as zero epoch timestamp.
    // It is mapped as Spark TimestampType but fixed at 1970-01-01 for day,
    // time portion is time of day, with no reference to a particular calendar,
    // time zone or date, with a precision till microseconds.
    // It stores the number of milliseconds after midnight, 00:00:00.000000
    case TimestampType if metadata.contains("logical_time_type") =>
      (rs: ResultSet, row: InternalRow, pos: Int) => {
        val rawTime = rs.getTime(pos + 1)
        if (rawTime != null) {
          val localTimeMicro = TimeUnit.NANOSECONDS.toMicros(
            rawTime.toLocalTime().toNanoOfDay())
          val utcTimeMicro = DateTimeUtils.toUTCTime(
            localTimeMicro, SQLConf.get.sessionLocalTimeZone)
          row.setLong(pos, utcTimeMicro)
        } else {
          row.update(pos, null)
        }
      }

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, rs.getBytes(pos + 1))

    case ArrayType(et, _) =>
      val elementConversion = et match {
        case TimestampType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
              nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
            }

        case StringType =>
          (array: Object) =>
            // some underling types are not String such as uuid, inet, cidr, etc.
            array.asInstanceOf[Array[java.lang.Object]]
              .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

        case DateType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Date]].map { date =>
              nullSafeConvert(date, DateTimeUtils.fromJavaDate)
            }

        case dt: DecimalType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
              nullSafeConvert[java.math.BigDecimal](
                decimal, d => Decimal(d, dt.precision, dt.scale))
            }

        case LongType if metadata.contains("binarylong") =>
          throw new IllegalArgumentException(s"Unsupported array element " +
            s"type ${dt.catalogString} based on binary")

        case ArrayType(_, _) =>
          throw new IllegalArgumentException("Nested arrays unsupported")

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val array = nullSafeConvert[java.sql.Array](
          input = rs.getArray(pos + 1),
          array => new GenericArrayData(elementConversion.apply(array.getArray)))
        row.update(pos, array)

    case _ => throw new SQLException(s"Unsupported type ${dt.catalogString}")
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }
}
