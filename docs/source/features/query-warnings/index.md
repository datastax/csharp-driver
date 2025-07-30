# Query warnings

When a query is considered harmful for the overall cluster, Cassandra writes a warning to the Cassandra logs. As of Cassandra 2.2, [these warnings are also returned to the client drivers](https://issues.apache.org/jira/browse/CASSANDRA-8930).

In the driver, these warnings are returned in the [RowSet][rowset-api] property `Info`. The warning is also written to the driver logs.

[rowset-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.RowSet.html