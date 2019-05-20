# Upgrade Guide

The purpose of this guide is to detail the changes made by the successive versions of the DataStax C# Driver that are relevant to for an upgrade from prior versions.

If you have any question or comment, you can [post it on the mailing list][mailing-list].

## Our policy regarding API changes and release versioning

The driver versions follow semantic versioning.

### Major releases

Example: `3.0.0`

Regarding `major` releases, any public component of the driver might be changed or removed although we will always try to avoid introducing significant changes that would make it significantly harder for an application to upgrade to the new `major` version.

### Minor releases

Example: `3.10.0`

For `minor` releases, it's a little more complicated because we don't want to be forced into bumping the `major` version every time we want to add a new client facing feature to the driver. For this reason, we group public interfaces of the driver in two categories. Here we refer to them as _mockable_ and _implementable_ but these names are here just to make it easier to explain them in this section.

#### _Implementable_ Interfaces

These interfaces exist to allow client applications to provide customized behavior that will be executed by the driver. For these interfaces, the driver provides ways for the applications to plug them in. Some examples: policies such as `ILoadBalancingPolicy` can be plugged in through builder methods like `Builder.WithLoadBalancingPolicy`, `ITimestampGenerator` can be plugged in through `Builder.WithTimestampGenerator`.

**In `minor` releases, these interfaces (_Implementable_) will not contain any changes to existing methods. Additionally, they will not receive new methods.**

The reason why we commit to never add new methods to these interfaces in `minor` releases is due to the fact that these interfaces are meant to be implemented by client applications while the remaining interfaces are not meant to be implemented.

#### _Mockable_ Interfaces

For the remaining policies, i.e., those who can not be plugged in to the driver, they exist to allow client applications to mock the driver in their test suites and inject those dependencies in the application when needed. These interfaces are usually the main entry points of the driver's public API with which client applications interact to execute requests. Some examples: `ISession`, `ICluster`, `IMapper`, `ICqlQueryAsyncClient`, `ICqlWriteAsyncClient`, `ICqlQueryClient`, `ICqlWriteClient`. These interfaces fall into this category (_Mockable_).

Other interfaces like `IStatement` and `ICqlBatch` can be passed as parameters to certain methods of the driver (`ISession.Execute(IStatement)`and `IMapper.Execute(ICqlBatch)`) but the client application will create these instances as part of the normal flow of execution through other methods:

- `IStatement` is created with the constructors of `SimpleStatement` and `BatchStatement` or with `PreparedStatement.Bind()` for instances of `BoundStatement`
- `ICqlBatch` is created with `IMapper.CreateBatch()`

Client applications shouldn't implement interfaces like `IStatement` and `ICqlBatch` and, therefore, these are also part of the _Mockable_ category.

**Users should expect new methods to be added to the interfaces that fall into this category (_Mockable_) in `minor` releases.**

We recommend users to use a mocking library that do not force applications to provide an implementation of every single method of an interface.

If you need to implement a wrapper class to provide functionality on top of the driver (like tracing), we recommend **composition** instead of **inheritance**, i.e., the wrapper class should have its own interface instead of implementing the driver interface.

### Patch releases

Example: `3.4.1`

These releases only contain bug fixes so they will never contain changes to the driver's public API.

## 3.10

A lot of new methods were added to some interfaces on this release, mostly to `ISession`, `IMapper` and `ICluster`. We are also taking this opportunity to clarify our stance on breaking changes. To read about our policy on public API changes read the first section on the top of this page.

### New methods with execution profile support

In order to implement Execution Profiles, it was necessary to add new methods to some interfaces/abstract classes. Here is the list of new methods and abstract methods for interfaces and abstract classes:

#### Data.Linq.Batch

- `abstract Task<RowSet> InternalExecuteAsync(string executionProfile)`

The new LINQ methods that provide execution profile support are not on this list because the driver's LINQ api doesn't have interfaces or abstract classes, except for `Batch` which is mentioned here.

#### ISession

- `RowSet Execute(IStatement statement, string executionProfileName)`
- `RowSet Execute(string cqlQuery, string executionProfileName)`
- `Task<RowSet> ExecuteAsync(IStatement statement, string executionProfileName)`

#### IMapper

- Execute batch
  - `void Execute(ICqlBatch batch, string executionProfile)`
  - `Task ExecuteAsync(ICqlBatch batch, string executionProfile)`
- Execute conditional batch
  - `Task<AppliedInfo<T>> ExecuteConditionalAsync<T>(ICqlBatch batch, string executionProfile)`
  - `AppliedInfo<T> ExecuteConditional<T>(ICqlBatch batch, string executionProfile)`
- Insert
  - `void Insert<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
  - `void Insert<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)`
  - `void Insert<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)`
  - `Task InsertAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
  - `Task InsertAsync<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)`
  - `Task InsertAsync<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)`
- Update
  - `void Update<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
  - `Task UpdateAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
- Delete
  - `void Delete<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
  - `Task DeleteAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
- Insert if not exists
  - `Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
  - `Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)`
  - `Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)`
  - `AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null)`
  - `AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null)`
  - `AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)`

Some methods didn't need an overload to support execution profiles because they have a `Cql` parameter and `Cql` has a new method called `WithExecutionProfile` that is meant to provide support for execution profiles in methods that have a `Cql` parameter.

### New methods not related to execution profiles

#### ICluster

All of these methods were already implemented in `Cluster` but were not declared in `ICluster`, except for `RefreshSchemaAsync` which is a new async version of the existing `RefreshSchema` method:

- `Task<ISession> ConnectAsync()`
- `Task<ISession> ConnectAsync(string keyspace)`
- `Task ShutdownAsync(int timeoutMs = Timeout.Infinite)`
- `Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)`
- `bool RefreshSchema(string keyspace = null, string table = null)`

#### ICqlBatch

This method wasn't declared in `ICqlBatch` by mistake since it was implemented in `CqlBatch`:

- `void InsertIfNotExists<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null)`

#### UdtMap\<T\>

A new optional parameter `keyspace` was added to the following method:

- `public static UdtMap<T> For<T>(string udtName = null, string keyspace = null)`

## 3.9

The `usedHostsPerRemoteDc` parameter on `DCAwareRoundRobinPolicy` is now deprecated. It will be removed in the following major version of the driver.

This parameter can give the indication that the driver will handle DC failover for you, but in virtually all cases this is inadequate. There are many considerations that are best handled at an operational/service level and not at client application level. Here are some examples of these considerations:
- Datacenter location
- Application being in the same failing region
- Local consistency levels in remote datacenter

A good write up on DC failover describing some of these considerations can be found [here](https://medium.com/@foundev/cassandra-local-quorum-should-stay-local-c174d555cc57).

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