# Execution Profiles

Execution profiles provide a mechanism to group together a set of configuration options and reuse them across different query executions. These options include:

- Load balancing policy
- Retry policy
- Speculative Execution policy
- Consistency level
- Serial consistency level
- Per-host request timeout

Execution profiles API is being introduced to help deal with the exploding number of configuration options, especially
as the database platform evolves into more complex workloads.

The legacy configuration remains intact but it is recommended to set the available options via the new Execution Profiles API.

This page explains how Execution Profiles relate to existing settings, and shows how to use the new profiles for
request execution.

## Using Execution Profiles

When SLAs are defined there might be different SLAs for different parts of a system. When it comes to authentication, for example, the SLA for a log in might be different from the SLA for a new user registration. Let's say that we need the following settings to meet the SLAs:

| User journey    | Speculative Execution Policy                 | Consistency |
|-----------------|----------------------------------------------|-------------|
| Log in          | `ConstantSpeculativeExecutionPolicy(100, 1)`  | LOCAL_ONE   |
| Sign up         | `NoSpeculativeExecutionPolicy`               | QUORUM      |

Note that `ConstantSpeculativeExecutionPolicy(100, 1)` means that at most `1` speculative execution will be launched after `100` ms of not receiving a response from a coordinator node. For more information on speculative executions, [see this page](../speculative-retries).

Instead of manually adjusting the options on every request, you can create execution profiles:

```csharp
var cluster =
   Cluster.Builder()
          .AddContactPoint("127.0.0.1")
          .WithExecutionProfiles(opts => opts
            .WithProfile("default", profile => profile
                .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc: "dc1"))))
            .WithProfile("login", profile => profile
                .WithConsistencyLevel(ConsistencyLevel.LocalOne)
                .WithSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(delay: 100, maxSpeculativeExecutions: 1)))
            .WithProfile("signup", profile => profile
                .WithConsistencyLevel(ConsistencyLevel.Quorum)
                .WithSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.Instance)))
          .Build();
```

Note that both profiles (`login` and `signup`) will inherit the unspecified parameters from the `default` profile. This means that in this case both profiles will use the token and datacenter aware load balancing policy with local datacenter `dc1`.

Now each request only needs a profile name. Here's an example with `Mapper`.

```csharp
// on startup
var session = cluster.Connect();
var mapper = new Mapper(session);

// on request
var cql = Cql.New("SELECT * FROM users WHERE username = ?", username).WithExecutionProfile("login");
var fetchResult = await mapper.FetchAsync<User>(cql).ConfigureAwait(false);
```

And here's the same operation with a `PreparedStatement` and `Session.ExecuteAsync`:

```csharp
// on startup
var session = cluster.Connect();
var ps = await session.PrepareAsync("SELECT * FROM users WHERE username = ?").ConfigureAwait(false);

// on request
var statement = ps.Bind(username);
var fetchResult = await session.ExecuteAsync(statement, "login").ConfigureAwait(false);
```

## Mapping Legacy Parameters to Profiles

The name `default` is reserved for the default execution profile. This profile will be the one that is going to be used whenever no profile is specified in a request.

You can change the default profile either by the legacy parameters on `Cluster.Builder` or by changing the execution profile itself with `Builder.WithExecutionProfiles`.

The following code snippet illustrates two `Cluster` instances being built with the same configuration parameters:

- `cluster1` uses the legacy configuration
- `cluster2` uses the new Execution Profile API for the same parameters as `cluster1`

```csharp
var cluster2 = 
   Cluster.Builder()
          .AddContactPoint("127.0.0.1")
          .WithQueryOptions(
              new QueryOptions()
                  .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                  .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
          .WithLoadBalancingPolicy(lbp)
          .WithSpeculativeExecutionPolicy(sep)
          .WithRetryPolicy(rp)
          .Build();

var cluster2 = 
   Cluster.Builder()
          .AddContactPoint("127.0.0.1")
          .WithExecutionProfiles(opts => opts
            .WithProfile("default", profile => profile
                .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)
                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                .WithLoadBalancingPolicy(lbp)
                .WithSpeculativeExecutionPolicy(sep)
                .WithRetryPolicy(rp)))
          .Build();
```

## Derived Execution Profiles

This is an advanced feature that might be useful in some cases. You can create derived profiles that inherit parameters from base profiles. A similar behavior is the way every execution profile inherits parameters from the `default` profile.

Let's say an application needs 2 execution profiles for its operations but it also implements datacenter failover. In this case it will need 2 more execution profiles that will basically be the same except for the `localDc` parameter of `DCAwareRoundRobinPolicy` and for the delay used in `ISpeculativeExecutionPolicy`.

| Scenario       | Speculative Execution Policy    | Consistency  | Local Datacenter |
|----------------|------------------|--------------|-----------------|
| default        | `ConstantSpeculativeExecutionPolicy(50, 1)`             | LOCAL_ONE    | dc1             |
| local-quorum   | `ConstantSpeculativeExecutionPolicy(100, 1)`            | LOCAL_QUORUM | dc1             |
| remote-one     | `ConstantSpeculativeExecutionPolicy(2000, 1)`           | LOCAL_ONE    | dc2             |
| remote-quorum  | `ConstantSpeculativeExecutionPolicy(2500, 1)`           | LOCAL_QUORUM | dc2             |

Here is how this looks in code (note that `local-quorum` is not created with `WithDerivedProfile`, because the `default` profile inheritance happens by default):

```csharp
var cluster = 
   Cluster.Builder()
          .AddContactPoint("127.0.0.1")
          .WithExecutionProfiles(opts => opts
            .WithProfile("default", profile => profile
                .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc: "dc1")))
                .WithConsistencyLevel(ConsistencyLevel.LocalOne)
                .WithSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(delay: 50, maxSpeculativeExecutions: 1)))
            .WithProfile("local-quorum", profile => profile
                .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)
                .WithSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(delay: 100, maxSpeculativeExecutions: 1)))
            .WithProfile("remote-one", profile => profile
                .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc: "dc2")))
                .WithConsistencyLevel(ConsistencyLevel.LocalOne)
                .WithSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(delay: 2000, maxSpeculativeExecutions: 1)))
            .WithDerivedProfile("remote-quorum", "remote-one", profile => profile
                .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)
                .WithSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(delay: 2500, maxSpeculativeExecutions: 1))))
          .Build();
```

## Accessing the defined profiles for a given Cluster

You can obtain `IReadOnlyDictionary` of immutable `IExecutionProfile` instances via the `Configuration.ExecutionProfiles` property.

```csharp
var profiles = cluster.Configuration.ExecutionProfiles;
```

Note that you can access the `ICluster` instance through `ISession`:

```csharp
var profiles = session.Cluster.Configuration.ExecutionProfiles;
```