# Core component

The core component is responsible for maintaining a pool of connections to the cluster and executes the statements based on client configuration.

Even though the core component allows low-level fine tuning, (for example, load-balancing policies to determine which node to use for each query), you interact using high-level objects like the `ISession` that represents a pool of connections to the cluster.

The other three components use the core component to execute statements and to handle the encoding and decoding of data.

## Quick Start

Here's a short program that connects to Cassandra and executes a query:

```csharp
var cluster = Cluster.Builder()
   .AddContactPoints("host1", "host2", "host3")
   .Build();                                                 // (1)
var session = cluster.Connect("sample_keyspace");            // (2)
var rs = session.Execute("SELECT * FROM sample_table");      // (3)
foreach (var row in rs)
{
   var value = row.GetValue<int>("sample_int_column");       // (4)
   //do something with the value
}
```

Brief description of this code snippet (see the number, e.g., `// (1)`):

1. `Cluster` is the main entry point of the driver. It holds the known state of the actual Cassandra cluster. It is thread-safe, you should create a single instance (per target Cassandra cluster), and share it throughout your application;
2. `Session` is what you use to execute queries. It is thread-safe, you should create a single instance (per `Cluster` instance), and share it throughout your application;
3. Here we use `Execute` to send a query to Cassandra. This returns a `RowSet`, which is an `IEnumerable` of `Row` objects. On the next line, we extract the first row (which is the only one in this case);
4. Extract the value of the first (and only) column from the row.

Always close the `Cluster` and `Session` once you're done with them, in order to free underlying resources (TCP connections, thread pools...). You can use `Dispose`, `Shutdown` or `ShutdownAsync`.

This example uses the synchronous API. Most methods have asynchronous equivalents (look for `*Async` variants that return a `Task`).
