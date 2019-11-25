# Core component

The core component is responsible for maintaining a pool of connections to the cluster and executes the statements based on client configuration.

Even though the core component allows low-level fine tuning, (for example, load-balancing policies to determine which node to use for each query), you interact using high-level objects like the `ISession` that represents a pool of connections to the cluster.

The other three components use the core component to execute statements and to handle the encoding and decoding of data.

## Example

```csharp
var cluster = Cluster.Builder()
   .AddContactPoints("host1", "host2", "host3")
   .Build();
var session = cluster.Connect("sample_keyspace");
var rs = session.Execute("SELECT * FROM sample_table");
foreach (var row in rs)
{
   var value = row.GetValue<int>("sample_int_column");
   //do something with the value
}
```
