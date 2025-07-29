# Tuning policies

## Load-balancing policy

The load balancing policy interface consists of three methods:

- `Distance(Host)`: determines the distance to the specified host. The values are `HostDistance.Ignored`, `Local` and `Remote`.
- `Initialize(Cluster)`: initializes the policy. The driver calls this method only once and before any other method calls are made.
- `NewQueryPlan()`: returns the hosts to use for a query. Each new query calls this method.

The driver includes these implementations:

- `DefaultLoadBalancingPolicy`
- `DCAwareRoundRobinPolicy`
- `RoundRobinPolicy`
- `TokenAwarePolicy`

### Default load-balancing policy

The default load-balancing policy is the `DefaultLoadBalancingPolicy`. For Cassandra workloads, its behavior is the same as an instance of `TokenAwarePolicy` with `DCAwareRoundRobinPolicy` as a child policy. It may seem complex but it actually isn't: The policy yields local replicas for a given key and, if not available, it yields nodes of the local datacenter in a round-robin manner.

To specify the **local datacenter** with the default load-balancing policy you can do this:

```csharp
Cluster.Builder()
       .AddContactPoint("127.0.0.1")
       .WithLoadBalancingPolicy(Policies.NewDefaultLoadBalancingPolicy("datacenter1"))
       .Build();
```

## Reconnection policy

The reconnection policy consists of one method:

- `NewSchedule()`: creates a new schedule to use in reconnection attempts.

By default, the driver uses an exponential reconnection policy. The driver includes these three policy classes:

- `ConstantReconnectionPolicy`
- `ExponentialReconnectionPolicy`
- `FixedReconnectionPolicy`

### FixedReconnectionPolicy sample

```csharp
// When building a cluster, set the reconnection policy to 
// Wait a few milliseconds to attempt first reconnection (400 ms) 
// Wait 5 seconds for the seconds reconnection attempt (5000 ms) 
// Wait 2 minutes for the third (2 * 60000 ms) 
// Wait 1 hour for the following attempts (60 * 60000 ms) 
Cluster.Builder()
   .WithReconnectionPolicy(new FixedReconnectionPolicy(400, 5000, 2 * 60000, 60 * 60000)
```

## Retry policy

A client may send requests to any node in a cluster whether or not it is a replica of the data being queried. This
node is placed into the coordinator role temporarily. Which node is the coordinator is determined by the load
balancing policy for the cluster. The coordinator is responsible for routing the request to the appropriate replicas.
If a coordinator fails during a request, the driver connects to a different node and retries the request. If the
coordinator knows before a request that a replica is down, it can throw an `UnavailableException`, but if the replica
fails after the request is made, it throws a `ReadTimeoutException` or `WriteTimeoutException`. Of course, this all
depends on the consistency level set for the query before executing it.

A retry policy centralizes the handling of query retries, minimizing the need for catching and handling of exceptions
in your business code.

The `IExtendedRetryPolicy` interface consists of four methods:

- `OnReadTimeout()`
- `OnUnavailable()`
- `OnWriteTimeout()`
- `OnRequestError()`

By default, the driver uses a default retry policy. The driver includes these five policy classes:

- `DefaultRetryPolicy`
- `DowngradingConsistencyRetryPolicy`
- `FallthroughRetryPolicy`
- `LoggingRetryPolicy`
- `IdempotenceAwareRetryPolicy`