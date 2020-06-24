# Upgrading to version 4

This guide is intended for users that are using the version 3 of the driver and plan to migrate to version 4.

## Local datacenter and contact points requirements

In order to improve the user experience for users who are just getting started with the driver, it is no longer required to provide contact points to the `Builder`. If no contact points are provided, the driver will use an implicit contact point which is `127.0.0.1:9042` (and a message will be logged at `INFO` level).

It is now required to provide the local datacenter to the `Builder`. There is one exception to this rule which is the case where no contact points are provided. When the implicit contact point is used, the driver will infer the local datacenter from that contact point.

You can provide the local datacenter in two ways:

- At builder level, using `Builder.WithLocalDatacenter()`
- At policy level, using `Policies.NewDefaultLoadBalancingPolicy()` or the constructors for `DCAwareRoundRobinPolicy` and `DefaultLoadBalancingPolicy`.

If you provide the local datacenter at both levels, the policy's configuration overrides the local datacenter set at builder level.

If you really need the legacy behavior of inferring the local datacenter from the contact points, the driver exposes a load balancing policy called `DcInferringLoadBalancingPolicy`. Note that this is is not recommended as it can lead to inconsistent behavior if contact points are not intentionally limited to a single, local datacenter.

## Removal of `usedHostsPerRemoteDC`

The `usedHostsPerRemoteDc` parameter on the `DCAwareRoundRobinPolicy` gave the indication that the driver would handle datacenter failover, but in virtually all cases this is inadequate. There are many considerations (datacenter location, application being in same failing region, local consistency levels in remote datacenter, etc.) that are best handled at an operational/service level and not at the client application level.

A good write up on datacenter failover describing some of these considerations can be found [here][dc-failover-post].

## Creation of `ISession` instances returns immediately

In C# driver 4.0, when you create an `ISession` instance, the initialization task will be started in the background. When a request is executed, the driver will wait for the initialization task to finish before sending the request.

If you want to explicitely wait for the initialization to be finished, you can use one of these new methods: `ISession.ConnectAsync()` / `ISession.Connect()`.

## Addition of several async methods (e.g. `ICluster.GetMetadataAsync()`)

The `ICluster.Metadata` property has always blocked until the initialization task is finished but until now there weren't many issues with this behavior because the user would create a session right away which would initialize the `ICluster` instance in a blocking manner.

Now that the initialization happens in the background, it's more likely that the user faces a scenario where the `ICluster.Metadata` blocks which is really bad if the application uses TPL (e.g. `async/await`) because it could block a thread of the shared threadpool.

To fix this, we added an `async` way to retrieve the `Metadata` instance: `ICluster.GetMetadataAsync()`.

If you would like to keep using the `ICluster.Metadata` property, please make sure that you explicitly initialize the session before accessing it in case your application uses `async/await` to avoid blocking the thread.

There were some methods that used the `ICluster.Metadata` property so we have added async variants of those methods as well:

| Existing method              | New async method              | Namespace             |
|------------------------------|-------------------------------|-----------------------|
| `CreateBatch(this ISession)` | `ISession.CreateBatchAsync()` | `Cassandra.Data.Linq` |
| `GetState(this ISession)`    | `ISession.GetStateAsync()`    | `Cassandra`           |

## `Metadata` API changes

Several methods of the `ICluster` interface were actually wrappers around methods that are implemented in the `Metadata` class. We decided to move all metadata related elements from `ICluster`/`ISession` to `Metadata` because of two reasons:

- Simplification of the driver's API, it makes more sense to have metadata related methods, properties and events on the `Metadata` class
- The cluster/session initialization happens in the background, which means that we would have to add a lot of `async` methods to the `ICluster` interface if we kept them (similar to what we did with `GetMetadataAsync()`)

If you are using one of theses elements that were moved to the `Metadata` class, you can use `ICluster.Metadata` or `ICluster.GetMetadataAsync()` to access it (see previous section for a brief explanation about this).

These are the methods, properties and events that were affected:

| Old API                            | New API                           |
|------------------------------------|-----------------------------------|
| `ICluster.AllHosts()`              | `Metadata.AllHosts()`             |
| `ICluster.GetHost()`               | `Metadata.GetHost()`              |
| `ICluster.GetReplicas()`           | `Metadata.GetReplicas()`          |
| `ICluster.RefreshSchema()`         | `Metadata.RefreshSchema()`        |
| `ICluster.RefreshSchemaAsync()`    | `Metadata.RefreshSchemaAsync()`   |
| `ICluster.HostAdded`               | `Metadata.HostAdded`              |
| `ICluster.HostRemoved`             | `Metadata.HostRemoved`            |
| `ISession.BinaryProtocolVersion`   | `Metadata.ProtocolVersion`        |

Note: `Metadata.ProtocolVersion` returns an `enum` instead of `int`, you can cast this `enum` to `int` if you need it (`ISession.BinaryProtocolVersion` did this internally).

## Removal of `ISession.WaitForSchemaAgreement()`

When a DDL request is executed, the driver will wait for schema agreement before returning control to the user. See `ProtocolOptions.MaxSchemaAgreementWaitSeconds` for more info.

If you want to manually check for schema agreement you can use the `Metadata.CheckSchemaAgreementAsync()` method.

## `Metadata` no longer implements `IDisposable`

The implementation of `IDisposable` was pretty much empty at this point so we decided to remove it.

We also removed `Metadata.ShutDown()` for the same reason.

## `ILoadBalancingPolicy` interface changes

You are only affected by these changes if you implemented a custom load balancing policy in your application instead of using one of those that are provided by the driver.

### `ILoadBalancingPolicy.Initialize()`

The `Initialize()` method is now `InitializeAsync()` and returns a `Task`. If the implementation is not async, we recommend returning `Task.CompletedTask` or `Task.FromResult(0)`.

 `InitializeAsync()` now has a `Metadata` parameter instead of `ICluster`. You can obtain the hosts collection and replicas using the `Metadata` class (see the earlier section related to `Metadata` API changes for more information on these changes).

### `ILoadBalancingPolicy.NewQueryPlan()`

The `NewQueryPlan()` method now has a `Metadata` parameter in addition to `string` (keyspace) and `IStatement`.

This is to simplify the process of implementing a custom load balancing policy. Previously, all implementations had to be stateful and threadsafe, i.e., the `cluster` object that was provided in the `Initialize()` was necessary in order to implement the `NewQueryPlan()` method.

Now you can build a completely stateless load balancing policy (which is guaranteed to be threadsafe) by obtaining the hosts / replicas via the `Metadata` parameter in the `NewQueryPlan()` method. In this scenario you can have an implementation of the `InitializeAsync()` method that just returns `Task.CompletedTask` or `Task.FromResult(0)`.

You can still build more complex load balancing policies that access some kind of metadata service for example by implementing the `InitializeAsync()` method.

[dc-failover-post]: https://medium.com/@foundev/cassandra-local-quorum-should-stay-local-c174d555cc57