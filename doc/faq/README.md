# FAQ

### Which versions of DSE does the driver support?

The driver supports versions from 4.8 to 5 of [DataStax Enterprise][dse].

### How can I upgrade from the Apache Cassandra driver to the DSE driver?

There is a section in the [Getting Started](../getting-started/) page.

## Should I create multiple `IDseSession` instances in my client application?

Normally you should use one `ISession` instance per application. You should share that instance between classes within
your application. In the case you are using CQL and Graph workloads on a single application, it is recommended that
you use 2 different instances.

### Can I use a single `IDseCluster` and `IDseSession` instance for graph and CQL?

It's currently not recommended, as different different workloads should be distributed across different datacenters
and the load balancing policy should select the appropriate coordinator for each workload.
We are planning to introduce execution profiles, that will allow you to use the same `IDseSession` instance
for all workloads.

### Should I dispose or shut down `IDseCluster` or `IDseSession` instances after executing a query?

No, only call `cluster.Shutdown()` once in your application's lifetime, normally when you shutdown your application.

## How can I enable logging in the driver?

The driver allows you to plug in any [`ILoggerProvider`][logging-api] implementation, like [NLog][nlog] and
[Serilog][serilog] implementations.

You should set the provider before initializing the cluster, using the `Diagnostics` class:

```csharp
// Use the provider you prefer, in this case NLog
ILoggerProvider provider = new NLogLoggerProvider();
// Add it before initializing the Cluster
Cassandra.Diagnostics.AddLoggerProvider(provider);
```

You can configure the log levels you want to output using the provider API.

Alternatively, if you don't want to use a `ILoggerProvider` implementation, the driver can expose log events using
the .NET Tracing API.

```csharp
// Specify the minimum trace level you want to see
Cassandra.Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
// Add a standard .NET trace listener
Trace.Listeners.Add(new ConsoleTraceListener());
```

## What is the recommended number of queries that a batch should contain?

It depends on the size of the requests and the number of tables affected by the BATCH. Large batches can
cause a lot of stress on the coordinator. Consider that Cassandra batches are not suitable for bulk loading, there
are dedicated tools for that. Batches allow you to group related updates in a single request, so keep the BATCH size
small (in the order of tens).

Starting from Cassandra version 2.0.8, the node issues a warning if the batch size is greater than 5K.

## What is the best way to retrieve multiple rows that contain large-sized blobs?

You can decrease the number of rows retrieved per page. By using the `SetPageSize()` method on a statement, you
instruct the driver to retrieve fewer rows per request (the default is 5000).

[logging-api]: https://github.com/aspnet/Logging
[nlog]: https://github.com/NLog/NLog.Extensions.Logging
[serilog]: https://github.com/serilog/serilog-extensions-logging