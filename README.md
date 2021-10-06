# SQL Server Change Tracking data source for Spark.
This library implements Spark Streaming data source for changes made in SQL Server tables with Change Tracking.

NOTE: you need to [enable Change Tracking for the given table](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server?view=sql-server-ver15) in order to be able to use this library.

## Sample usage
The following snipped causes Spark to keep watching `MyDatabase.MyTable` for changes. It will try to group `100` changes per batch (best effort). It returns a data frame with the following columns:
  * `SYS_CHANGE_VERSION` and `SYS_CHANGE_OPERATION`
  * Primary keys (never `NULL`, even when the row has been deleted)
  * Other columns (whose values are all `NULL` for deleted rows)

```scala
val df = spark
    .readStream
    .format("org.apache.spark.sql.execution.streaming.sources.mssqlct.ChangeTrackingProvider")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", "jdbc:sqlserver://host.domain.tld;databaseName=MyDatabase;")
    .option("dbtable", "MyTable")
    .option("primaryKeyColumns", "MyPrimaryKey")
    .option("suggestedMaxChanges", "100")
    .load()

df.writeStream
    .outputMode("append")
    .option("truncate", false)
    .format("console")
    .start()
    .awaitTermination()
```

## How it works
This library queries `CHANGETABLE(CHANGES [MyTable], ...)` special table to list changes to made to `MyTable` and uses primary key columns to join columns form `MyTable`.

Check [SQL Server Change Tracking documentation for details](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server?view=sql-server-ver15#how-change-tracking-works).
