# Upgrade Guide to 2.0

The purpose of this guide is to detail the changes made by the version 2.0 of the DataStax C# Driver that are relevant to an upgrade from version 1.0.

We used the opportunity of a major version bump to incorporate your feedback, improve the API and to fix a number of inconsistencies. Unfortunately this means there are some breaking changes, but the new API should be both simpler and more complete.

## Breaking API Changes

1. The `Query` class has been renamed into `SimpleStatement`.

1. Created the interface `IStatement` which `SimpleStatement`, `BoundStatement` and `BatchStatement` implement.
The main `Session.Execute` and `Session.ExecuteAsync` methods use `IStatement` as parameter.

1. `RowSet` uses a queue internally and it dequeues items as you iterate through the rows (similarly to the [Java driver][1]).
This behaviour enables to have a stable memory use when paging through a large result.
If you still want to consume the `RowSet` as a list, you can use [.NET Enumerable.ToList][2] extension method, for example: `var rowList = rs.ToList();`.

1. `Session` implements `ISession` interface, to make unit test and mocking easier. `Cluster.Connect` now returns a `ISession` (it will still be a `Session` instance).

1. Policies (load balancing and retry policies) depend on `IStatement` and `ICluster` interfaces instead of solid classes.
This would only affect you only if you implemented a custom Policy.

1. `Context` class, the implementation of the Entity Framework [ObjectContext][context] that used to be under the `Cassandra.Data.Linq` namespace is now moved to the `Cassandra.Data.EntityContext` namespace and it is published in a separate Nuget package, named [CassandraEntityContext][entitynuget].


## Other API changes (non - breaking)

1. `RowSet` is now `IEnumerable<Row>`. You can iterate through the rows using the RowSet without having to invoke `RowSet.GetRows` method.

1. The is no need to call to the `Dispose` method of `RowSet`. `Dispose` method is now marked as obsolete and it will be removed in future versions.

1. Paging results: SELECT queries are now "paged" under the hood. In other words, if a query yields a very large result, only an initial amount of rows will be fetched (according to the page size), the rest of the rows will be fetched "on-demand" as you iterate through it.

_If you have any question or comment, please [post it on the mailing list][3]._


  [1]: https://github.com/datastax/java-driver
  [2]: http://msdn.microsoft.com/en-us/library/vstudio/bb342261(v=vs.100).aspx "Enumerable.ToList<TSource> Method"
  [3]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user "DataStax C# driver for Cassandra mailing list" 
  [context]: http://msdn.microsoft.com/en-us/library/system.data.objects.objectcontext(v=vs.110).aspx
  [entitynuget]: http://www.nuget.org/packages/CassandraEntityContext/