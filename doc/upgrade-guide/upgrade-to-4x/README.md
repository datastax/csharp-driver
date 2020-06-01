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

[dc-failover-post]: https://medium.com/@foundev/cassandra-local-quorum-should-stay-local-c174d555cc57
