# Upgrade Guide

## DataStax drivers unification

As of the 3.13.0 release of the [DataStax C# Driver for Apache Cassandra®](https://docs.datastax.com/en/developer/csharp-driver/latest/), the DataStax Enterprise C# Driver functionality has been merged into this single, DataStax driver. Please refer to [the blog post](https://www.datastax.com/blog/2020/01/better-drivers-for-cassandra) for more information on this change.

---

The purpose of this guide is to detail the changes made by the successive versions of the DataStax C# Driver that are relevant to for an upgrade from prior versions.

If you have any question or comment, you can [post it on the mailing list][mailing-list].

## Our policy regarding API changes and release versioning

The driver versions follow semantic versioning.

### Major releases

Example: `2.0.0`

Regarding `major` releases, any public component of the driver might be changed or removed although we will always try to avoid introducing significant changes that would make it significantly harder for an application to upgrade to the new `major` version.

### Minor releases

Example: `2.7.0`

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

Example: `2.1.1`

These releases only contain bug fixes so they will never contain changes to the driver's public API.

## 2.7

A lot of new methods were added to some interfaces on this release, mostly to `ISession`, `IMapper` and `ICluster`. Note that `IDseCluster` inherits from `ICluster` and `IDseSession` inherits from `ISession` so these are affected as well. We are also taking this opportunity to clarify our stance on breaking changes. To read about our policy on public API changes read the first section on the top of this page.

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

## 2.6

The `usedHostsPerRemoteDc` parameter on `DCAwareRoundRobinPolicy` is now deprecated. It will be removed in the following major version of the driver.

This parameter can give the indication that the driver will handle DC failover for you, but in virtually all cases this is inadequate. There are many considerations that are best handled at an operational/service level and not at client application level. Here are some examples of these considerations:
- Datacenter location
- Application being in the same failing region
- Local consistency levels in remote datacenter

A good write up on DC failover describing some of these considerations can be found [here](https://medium.com/@foundev/cassandra-local-quorum-should-stay-local-c174d555cc57).

## 2.3

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

[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user