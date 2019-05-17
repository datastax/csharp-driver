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

## Mapping Legacy Parameters to Profiles

The name "default" is reserved for the default execution profile. This profile will be the one that is going to be used whenever no profile is specified in a request.

You can change the default profile either by the legacy parameters on `Cluster.Builder` or by changing the execution profile itself with `Builder.WithExecutionProfiles`.

The following code snippet illustrates two `Cluster` instances being build with the same configuration parameters:

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
          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(9999))
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
                .WithReadTimeoutMillis(9999)
                .WithLoadBalancingPolicy(lbp)
                .WithSpeculativeExecutionPolicy(sep)
                .WithRetryPolicy(rp)))
          .Build();
```

## Using Execution Profiles

### Initializing cluster with profiles

Execution profiles should be created when creating the `Client` instance with a name that identifies it and the settings
that apply to the profile.

```javascript
const aggregationProfile = new ExecutionProfile('aggregation', {
  consistency: consistency.localQuorum,
  loadBalancing: new DCAwareRoundRobinPolicy('us-west'),
  retry: myRetryPolicy,
  readTimeout: 30000,
  serialConsistency: consistency.localSerial
});

const client = new Client({ 
  contactPoints: ['host1'],
  localDataCenter,
  profiles: [ aggregationProfile ]
});
```

Note that while the above options are all the supported settings on the execution profiles, you can specify only the
ones that are required for the executions, using the `'default'` profile to fill the rest of the options.

#### Default execution profile

You can define a default profile, using the name `'default'`:

```javascript
const client = new Client({ 
  contactPoints: ['host1'],
  localDataCenter,
  profiles: [ 
    new ExecutionProfile('default', {
      consistency: consistency.one,
      readTimeout: 10000
    }),
    new ExecutionProfile('time-series', {
      consistency: consistency.localQuorum
    })
  ]
});
```

The default profile will be used to fill the unspecified options in the rest of the profiles. In the above example, the
read timeout for the profile named `'time-series'` will be the one defined in the default profile (10,000 ms).

For the settings that are not specified in the default profile, the driver will use the default `Client` options.

### Using an execution profile by name

Use the name to specify which profile you want to use for the execution.

```javascript
client.execute(query, params, { executionProfile: 'aggregation' });
```
### Using an execution profile by instance

You can also use the `ExecutionProfile` instance.

```javascript
client.execute(query, params, { executionProfile: aggregationProfile });
```

### Using default execution profile

When the execution profile is not provided in the options, the default execution profile is used.

```javascript
client.execute(query, params);
```









Execution Profiles
==================

Execution profiles aim at making it easier to execute requests in different ways within
a single connected ``Session``. Execution profiles are being introduced to deal with the exploding number of
configuration options, especially as the database platform evolves more complex workloads.

The legacy configuration remains intact, but legacy and Execution Profile APIs
cannot be used simultaneously on the same client ``Cluster``. Legacy configuration
will be removed in the next major release (4.0).

This document explains how Execution Profiles relate to existing settings, and shows how to use the new profiles for
request execution.

Mapping Legacy Parameters to Profiles
-------------------------------------

Execution profiles can inherit from :class:`.cluster.ExecutionProfile`, and currently provide the following options,
previously input from the noted attributes:

- load_balancing_policy - :attr:`.Cluster.load_balancing_policy`
- request_timeout - :attr:`.Session.default_timeout`, optional :meth:`.Session.execute` parameter
- retry_policy - :attr:`.Cluster.default_retry_policy`, optional :attr:`.Statement.retry_policy` attribute
- consistency_level - :attr:`.Session.default_consistency_level`, optional :attr:`.Statement.consistency_level` attribute
- serial_consistency_level - :attr:`.Session.default_serial_consistency_level`, optional :attr:`.Statement.serial_consistency_level` attribute
- row_factory - :attr:`.Session.row_factory` attribute

When using the new API, these parameters can be defined by instances of :class:`.cluster.ExecutionProfile`.

Using Execution Profiles
------------------------
Default
~~~~~~~

.. code:: python

    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect()
    local_query = 'SELECT rpc_address FROM system.local'
    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query)[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.2')
    Row(rpc_address='127.0.0.1')


The default execution profile is built from Cluster parameters and default Session attributes. This profile matches existing default
parameters.

Initializing cluster with profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from cassandra.cluster import ExecutionProfile
    from cassandra.policies import WhiteListRoundRobinPolicy

    node1_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']))
    node2_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.2']))

    profiles = {'node1': node1_profile, 'node2': node2_profile}
    session = Cluster(execution_profiles=profiles).connect()
    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query, execution_profile='node1')[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.1')
    Row(rpc_address='127.0.0.1')


.. code:: python

    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query, execution_profile='node2')[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.2')
    Row(rpc_address='127.0.0.2')


.. code:: python

    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query)[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.2')
    Row(rpc_address='127.0.0.1')

Note that, even when custom profiles are injected, the default ``TokenAwarePolicy(DCAwareRoundRobinPolicy())`` is still
present. To override the default, specify a policy with the :data:`~.cluster.EXEC_PROFILE_DEFAULT` key.

.. code:: python

    from cassandra.cluster import EXEC_PROFILE_DEFAULT
    profile = ExecutionProfile(request_timeout=30)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})


Adding named profiles
~~~~~~~~~~~~~~~~~~~~~

New profiles can be added constructing from scratch, or deriving from default:

.. code:: python

    locked_execution = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']))
    node1_profile = 'node1_whitelist'
    cluster.add_execution_profile(node1_profile, locked_execution)
    
    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query, execution_profile=node1_profile)[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.1')
    Row(rpc_address='127.0.0.1')

See :meth:`.Cluster.add_execution_profile` for details and optional parameters.

Passing a profile instance without mapping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We also have the ability to pass profile instances to be used for execution, but not added to the mapping:

.. code:: python

    from cassandra.query import tuple_factory
    
    tmp = session.execution_profile_clone_update('node1', request_timeout=100, row_factory=tuple_factory)

    print session.execute(local_query, execution_profile=tmp)[0]
    print session.execute(local_query, execution_profile='node1')[0]

.. parsed-literal::

    ('127.0.0.1',)
    Row(rpc_address='127.0.0.1')

The new profile is a shallow copy, so the ``tmp`` profile shares a load balancing policy with one managed by the cluster.
If reference objects are to be updated in the clone, one would typically set those attributes to a new instance.








Execution profiles

Imagine an application that does both transactional and analytical requests. Transactional requests are simpler and must return quickly, so they will typically use a short timeout, let's say 100 milliseconds; analytical requests are more complex and less frequent so a higher SLA is acceptable, for example 5 seconds. In addition, maybe you want to use a different consistency level.

Instead of manually adjusting the options on every request, you can create execution profiles:

datastax-java-driver {
  profiles {
    oltp {
      basic.request.timeout = 100 milliseconds
      basic.request.consistency = ONE
    }
    olap {
      basic.request.timeout = 5 seconds
      basic.request.consistency = QUORUM
    }
}

Now each request only needs a profile name:

SimpleStatement s =
  SimpleStatement.builder("SELECT name FROM user WHERE id = 1")
      .setExecutionProfileName("oltp")
      .build();
session.execute(s);

The configuration has an anonymous default profile that is always present. It can define an arbitrary number of named profiles. They inherit from the default profile, so you only need to override the options that have a different value.