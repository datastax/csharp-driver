# Upgrading to version 4

This guide is intended for users that are using the version 3 of the driver and plan to migrate to version 4.

## Local datacenter and contact points requirements

In order to improve the user experience for users who are just getting started with the driver, it is no longer required to provide contact points to the `Builder`. If no contact points are provided, the driver will use an implicit contact point which is `127.0.0.1:9042` (and a message will be logged at `INFO` level).

It is now required to provide the local datacenter to the `Builder`. There is one exception to this rule which is the case where no contact points are provided. When the implicit contact point is used, the driver will infer the local datacenter from that contact point.

You can provide the local datacenter in two ways:

- At builder level, using `Builder.WithLocalDatacenter()`
- At policy level, using `Policies.NewDefaultLoadBalancingPolicy()` or the constructors for `DCAwareRoundRobinLoadBalancingPolicy` and `DefaultLoadBalancingPolicy`.

If you provide the local datacenter at both levels, the policy's configuration overrides the local datacenter set at builder level.
