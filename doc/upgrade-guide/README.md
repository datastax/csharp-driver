# Upgrade Guide

The purpose of this guide is to detail the changes made by the successive versions of the DataStax C# Driver that 
are relevant to for an upgrade from prior versions.

If you have any question or comment, you can [post it on the mailing list][mailing-list].

## 3.6

The `DowngradingConsistencyRetryPolicy` is now deprecated. It will be removed in the following major version of the 
driver.

The main motivation is the agreement that this policy's behavior should be the application's concern, not the driver's.

We recognize that there are use cases where downgrading is good – for instance, a dashboard application would present
the latest information by reading at `QUORUM`, but it's acceptable for it to display stale information by reading at
ONE sometimes.

But APIs provided by the driver should instead encourage idiomatic use of a distributed system like Apache Cassandra,
and a downgrading policy works against this. It suggests that an anti-pattern such as "try to read at `QUORUM`, but
fall back to `ONE` if that fails" is a good idea in general use cases, when in reality it provides no better consistency
guarantees than working directly at `ONE`, but with higher latencies.

We therefore urge users to carefully choose upfront the consistency level that works best for their use cases, and
should they decide that the downgrading behavior of `DowngradingConsistencyRetryPolicy` remains a good fit for certain
use cases, they will now have to implement this logic themselves, either at application level, or alternatively at
driver level, by rolling out their own downgrading retry policy.

## 2.5

### API changes

1. `Host.Address` field is now an `IPEndPoint` (IP address and port number) instead of an `IPAddress`.

1. There is one assembly delivered in the package (Cassandra.dll) and it is now strong-named.

1. Linq changes (see below).

### Linq changes

Even though there are no breaking changes regarding Linq in 2.5, it is important that you read this guide in order to
understand the new ways to use Linq and the Mapper components.

#### Fluent mapping definitions

Previously, the only way to provide mapping information to the Linq component was using class and method attribute decoration.
Now, you can define mapping using a fluent interface.

```csharp
MappingConfiguration.Global.
  Define(new Map<User>().TableName("users"));
```

Additionally, you can now share the same mapping configuration between the new Mapper and Linq.

#### Case sensitivity

Prior to version 2.5, Linq component used case-sensitive identifiers when generating CQL code. Now, the case 
sensitivity can be specified on the mapping information.

Using fluent configuration:

```csharp
var map = new Map<User>.TableName("Users").CaseSensitive();
MappingConfiguration.Global.Define(map);
```

#### Mapping attributes

Even though using the fluent interface over attribute decoration for mapping definition is encouraged, there are a
new set of attributes declared in the `Cassandra.Mapping.Attributes` namespace that can be used to define mapping
between your objects and Cassandra tables. These attributes can provide the Mapper and Linq components with all the
necessary information.

The former set of attributes located in the `Cassandra.Data.Linq` namespace are still present in version 2.5 of the
driver but they are only valid for the Linq component and they are now deprecated.

#### Linq IQueryable instance

There is a new way to obtain IQueryable instances that can be used to query Cassandra, that is using the `Table<T>`
constructor. Using the `Table<T>` constructor is an inexpensive operation and `Table<T>` instances can be created and
dereferenced without much overhead.

Example 1:

Creates a new instance of the Linq IQueryProvider using the global mapping configuration.

```csharp
var users = new Table<User>(session);
```

Example 2:

Creates a new instance of the Linq `IQueryProvider` using the mapping configuration provided.

```csharp
var config = new MappingConfiguration();
var users = new Table<User>(session, config);
```

The `ISession` extension method  `GetTable<T>()` used to obtain a `IQueryable` instance prior to 2.5 is still available,
internally it will call the new `Table<T>` constructors.

## 2.0

We used the opportunity of a major version bump to incorporate your feedback, improve the API and to fix a number
of inconsistencies. Unfortunately this means there are some breaking changes, but the new API should be both simpler
and more complete.

### Breaking API Changes

1. The `Query` class has been renamed into `SimpleStatement`.

1. Created the interface `IStatement` which `SimpleStatement`, `BoundStatement` and `BatchStatement` implement.
The main `Session.Execute` and `Session.ExecuteAsync` methods use `IStatement` as parameter.

1.  `RowSet` uses a queue internally and it dequeues items as you iterate through the rows .

    This behaviour enables to have a stable memory use when paging through a large result.

    If you still want to consume the `RowSet` as a list, you can use [.NET Enumerable.ToList][enum-tolist] extension 
    method, for example: `var rowList = rs.ToList();`.

1. `Session` implements `ISession` interface, to make unit test and mocking easier. `Cluster.Connect` now returns a
`ISession` (it will still be a `Session` instance).

1. Policies (load balancing and retry policies) depend on `IStatement` and `ICluster` interfaces instead of solid
classes. This would only affect you only if you implemented a custom Policy.

1. `Context` class, the implementation of the Entity Framework [ObjectContext][context] that used to be under the
`Cassandra.Data.Linq` namespace is now moved to the `Cassandra.Data.EntityContext` namespace and it is published in
a separate Nuget package, named [CassandraEntityContext][entitynuget].

1. `RowSet` is now `IEnumerable<Row>`. You can iterate through the rows using the RowSet without having to invoke
`RowSet.GetRows` method.

1. The is no need to call to the `Dispose` method of `RowSet`. `Dispose` method is now marked as obsolete and it will
be removed in future versions.

1. Paging results: SELECT queries are now "paged" under the hood. In other words, if a query yields a very large result,
only an initial amount of rows will be fetched (according to the page size), the rest of the rows will be fetched
"on-demand" as you iterate through it.


[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user
[context]: https://msdn.microsoft.com/en-us/library/system.data.objects.objectcontext(v=vs.110).aspx
[entitynuget]: https://www.nuget.org/packages/CassandraEntityContext/
[enum-tolist]: https://msdn.microsoft.com/en-us/library/bb342261(v=vs.110).aspx