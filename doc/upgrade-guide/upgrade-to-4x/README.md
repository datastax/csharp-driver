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

## Session initialization retries

When the control connection initialization fails because it wasn't able to reach any of the contact points, the driver will retry the initialization according to the configured reconnection policy.

While an initialization attempt is in progress, the session methods that require initialization (e.g. `session.Execute()`) block until it is finished. If the initialization fails, the methods that were blocked will throw an exception. Until a new initialization attempt begins, any call to a method that requires initialization will throw the same exception.

## Addition of several async methods (e.g. `Metadata.AllHostsAsync()`)

The `ICluster.Metadata` property has always blocked until the initialization task is finished but until now there weren't many issues with this behavior because the user would create a session right away which would initialize the `ICluster` instance in a blocking manner.

Now that the initialization happens in the background, the `ICluster.Metadata` property no longer blocks but the methods on `IMetadata` block until the initialization is finished. For users who use `async/await` we added `async` variants for all methods (e.g. `Metadata.AllHostsAsync()`).

Some methods were added that return a snapshot of the current metadata cache (e.g. `Metadata.AllHostsSnapshot()`). These methods do not block but will return empty collections if the initialization is not done:

- `IMetadata.AllHostsSnapshot()`
- `IMetadata.AllReplicasSnapshot()`
- `IMetadata.GetReplicasSnapshot()`

There are also some extension methods that require a session to be initialized so we have added async variants of those methods as well:

| Existing method              | New async method              | Namespace             |
|------------------------------|-------------------------------|-----------------------|
| `CreateBatch(this ISession)` | `ISession.CreateBatchAsync()` | `Cassandra.Data.Linq` |
| `GetState(this ISession)`    | `ISession.GetStateAsync()`    | `Cassandra`           |

There are also some properties that were moved to the `ClusterDescription` class. You can obtain a `ClusterDescription` instance via the `IMetadata.GetClusterDescriptionAsync()` method (or `IMetadata.GetClusterDescription()`).

| `Metadata.ClusterName`       | `ClusterDescription.ClusterName`   | `Cassandra`           |
| `Metadata.IsDbaas`           | `ClusterDescription.IsDbaas`       | `Cassandra`           |

## `Metadata` API changes

Several methods of the `ICluster` interface were actually wrappers around methods that are implemented in the `Metadata` class. We decided to move all metadata related elements from `ICluster`/`ISession` to the new `IMetadata` interface to simplify the driver's API, it makes more sense to have metadata related methods, properties and events on the `IMetadata` interface.

If you are using one of theses elements that were moved to the `IMetadata` interface, you can use `ICluster.Metadata` to access it. Note that some of these methods now block until the initialization is done so we added `async` variants for them (see previous section for an explanation about this).

These are the methods, properties and events that were affected:

| Old API                            | New API                           |
|------------------------------------|-----------------------------------|
| `ICluster.AllHosts()`              | `IMetadata.AllHosts()`             |
| `ICluster.GetHost()`               | `IMetadata.GetHost()`              |
| `ICluster.GetReplicas()`           | `IMetadata.GetReplicas()`          |
| `ICluster.RefreshSchema()`         | `IMetadata.RefreshSchema()`        |
| `ICluster.RefreshSchemaAsync()`    | `IMetadata.RefreshSchemaAsync()`   |
| `ICluster.HostAdded`               | `IMetadata.HostAdded`              |
| `ICluster.HostRemoved`             | `IMetadata.HostRemoved`            |
| `ISession.BinaryProtocolVersion`   | `IMetadata.GetClusterDescription().ProtocolVersion`        |

Note: `ClusterDescription.ProtocolVersion` returns an `enum` instead of `int`, you can cast this `enum` to `int` if you need it (`ISession.BinaryProtocolVersion` did this internally).

## Removal of `ISession.WaitForSchemaAgreement()`

When a DDL request is executed, the driver will wait for schema agreement before returning control to the user. See `ProtocolOptions.MaxSchemaAgreementWaitSeconds` for more info.

If you want to manually check for schema agreement you can use the `IMetadata.CheckSchemaAgreementAsync()` method.

## `Metadata` no longer implements `IDisposable`

The implementation of `IDisposable` was pretty much empty at this point so we decided to remove it.

We also removed `Metadata.ShutDown()` for the same reason.

## `ILoadBalancingPolicy` interface changes

You are only affected by these changes if you implemented a custom load balancing policy in your application instead of using one of those that are provided by the driver.

### `ILoadBalancingPolicy.Initialize()`

The `Initialize()` method is now `InitializeAsync()` and returns a `Task`. If the implementation is not async, we recommend returning `Task.CompletedTask` or `Task.FromResult(0)`.

 `InitializeAsync()` now has a `IMetadataSnapshotProvider` parameter instead of `ICluster`. You can obtain the hosts collection and replicas using this instance (see the earlier section related to `Metadata` API changes for more information on these changes).

### `ILoadBalancingPolicy.NewQueryPlan()` and `ILoadBalancingPolicy.Distance()`

The `NewQueryPlan()` and `Distance()` methods now have a `ICluster` parameter.

This is to simplify the process of implementing a custom load balancing policy. Previously, all implementations had to be stateful and threadsafe, i.e., the `cluster` object that was provided in the `Initialize()` was necessary in order to implement the `NewQueryPlan()` method.

Now you can build a completely stateless load balancing policy (which is guaranteed to be threadsafe) by obtaining the hosts / replicas via the `ICluster` parameter in the `NewQueryPlan()` method. In this scenario you can have an implementation of the `InitializeAsync()` method that just returns `Task.CompletedTask` or `Task.FromResult(0)`.

You can still build more complex load balancing policies that access some kind of metadata service for example by implementing the `InitializeAsync()` method.

[dc-failover-post]: https://medium.com/@foundev/cassandra-local-quorum-should-stay-local-c174d555cc57