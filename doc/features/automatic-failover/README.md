# Automatic failover

If a Cassandra node fails or becomes unreachable, the C# driver automatically and transparently tries other nodes in
the cluster and schedules reconnections to the dead nodes in the background.

How the driver handles failover is determined by which retry and reconnection policies are used when building a
`Cluster` instance.

For more information on how to change the Policies used by the driver check out the ["Tuning policies" page](../tuning-policies).

## Example

This code illustrates building a `ICluster` instance with a retry policy that logs every retry decision with `LogLevel.Info`.

```csharp
   ICluster cluster = Cluster.Builder()
         .AddContactPoints("127.0.0.1", "127.0.0.2")
         .WithRetryPolicy(new LoggingRetryPolicy(new DefaultRetryPolicy()))
         .Build();
   ISession session = cluster.Connect();
```