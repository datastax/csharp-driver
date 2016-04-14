# Automatic failover

If a Cassandra node fails or becomes unreachable, the C# driver automatically and transparently tries other nodes in
the cluster and schedules reconnections to the dead nodes in the background.

How the driver handles failover is determined by which retry and reconnection policies are used when building a
`Cluster` instance.

## Example

This code illustrates building a `ICluster` instance with a retry policy which sometimes retries with a lower
consistency level than the one specified for the query.

```csharp
   ICluster cluster = Cluster.Builder()
         .AddContactPoints("127.0.0.1", "127.0.0.2")
         .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
         .WithReconnectionPolicy(new ConstantReconnectionPolicy(1000L))
         .Build();
   ISession session = cluster.Connect();
```