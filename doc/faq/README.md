# FAQ

## Should I create multiple ISession instances in my client application?

Normally you should use one `ISession` instance per application. You should share that instance between classes within
your application.

## How can I enable tracing in the driver?

```csharp
// Specify the minimum trace level you want to see
Cassandra.Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
// Add a standard .NET trace listener
Trace.Listeners.Add(new ConsoleTraceListener());
```

## What is the recommended number of queries that a batch should contain?

It depends on the size of the requests and the number of tables affected by the BATCH. Large batches can
cause a lot of stress on the coordinator. Consider that Cassandra batches are not suitable for bulk loading, there
are dedicated tools for that. batches allow you to group related updates in a single request, so keep the BATCH size
small (in the order of tens).

Starting from Cassandra version 2.0.8, the node issues a warning if the batch size is greater than 5K.

## What is the best way to retrieve multiple rows that contain large-sized blobs?

You can decrease the number of rows retrieved per page. By using the `SetPageSize()` method on a statement, you
instruct the driver to retrieve fewer rows per request (the default is 5000).