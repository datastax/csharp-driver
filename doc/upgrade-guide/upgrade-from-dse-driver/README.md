# Upgrading from the DSE Driver

This guide is intended for users of the DSE driver that plan to migrate to this driver, i.e., `CassandraCSharpDriver`.

This guide was written with version 3 of the unified driver in mind but most of it is valid for version 4. There's also [this guide](../upgrade-to-4x) that helps users update from version 3 to version 4 so DSE driver users should find it useful since the unified driver 3 shares most of its API with DSE Driver 2.

This driver now (since version `3.13`) supports all DataStax products and features, such as Unified Authentication, Kerberos, geo types and graph traversal executions, allowing you to use a single driver for Apache Cassandra, DSE or other DataStax products.

## Changes to default settings

The default retry policy in `DseClusterBuilder` was `new IdempotencyAwareRetryPolicy(new DefaultRetryPolicy())`. With `Builder` the default is `new DefaultRetryPolicy()`.

## Removed classes and interfaces

`DseLoadBalancingPolicy` is now `DefaultLoadBalancingPolicy`. This is the new default load balancing policy in this driver. The behavior is the same as the previous default policy except for some DSE specific workloads so there is no impact for existing applications.

`DseLoadBalancingPolicy.CreateDefault` has been replaced by `Policies.DefaultLoadBalancingPolicy` and `Policies.NewDefaultLoadBalancingPolicy`.

It is no longer possible to pass a custom child policy to `DefaultLoadBalancingPolicy` like it was possible with `DseLoadBalancingPolicy` on the DSE driver.

`DseClusterBuilder` has been removed. These builder methods were moved to `Builder`. Note that `DseClusterBuilder.WithCredentials` added an instance of `DsePlainTextAuthenticator` but `Builder.WithCredentials` adds an instance of `PlainTextAuthenticator`. If you wish to use `DsePlainTextAuthenticator` you need to use another builder method: `Builder.WithAuthProvider(new DsePlainTextAuthenticator(username, password))`.

`DseConfiguration` has been removed. These class properties were moved to `Configuration`.

`IDseSession`/`DseSession` has been removed. `ExecuteGraph` and `ExecuteGraphAsync` methods were moved to `ISession`/`Session`.

`IDseCluster`/`DseCluster` have been removed. Use `ICluster`/`Cluster` instead.

The namespaces associated with Auth, Graph and Search were moved:

|`Dse` namespace  | `Cassandra` namespace  |
|--|--|
|`Dse.Auth`|`Cassandra.DataStax.Auth`|
|`Dse.Graph`|`Cassandra.DataStax.Graph`|
|`Dse.Search`|`Cassandra.DataStax.Search`|