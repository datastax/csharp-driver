# Getting started

## DataStax drivers unification

As of the 3.13.0 release of the [DataStax C# Driver for Apache Cassandra®](https://docs.datastax.com/en/developer/csharp-driver/latest/), the DataStax Enterprise C# Driver functionality has been merged into this single, DataStax driver. Please refer to [the blog post](https://www.datastax.com/blog/2020/01/better-drivers-for-cassandra) for more information on this change.

---

Getting started with the DataStax Enterprise C# Driver.

## Installation

[Get it on Nuget][nuget]

```
PM> Install-Package Dse
```

## Upgrading from the core driver

To Upgrade from `CassandraCSharpDriver` to `Dse`, replace `using` directive to point to `Dse` instead of `Cassandra`.

```csharp
using Dse;
```

And create `IDseCluster` and `IDseSession` instances.

```csharp
IDseCluster cluster = DseCluster.Builder()
                                .AddContactPoint("127.0.0.1")
                                .Build();
IDseSession session = cluster.Connect();
```

## Execute CQL queries

`IDseSession` extends the core driver counterpart `ISession`, so you can use `IDseSession` instances to execute CQL
queries.

```csharp
RowSet rs = session.Execute("select * from system.local");
```

## Execute Graph queries

Additionally, `IDseSession` exposes graph-specific methods.

```csharp
GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
```

[nuget]: https://nuget.org/packages/Dse/
[dse]: http://www.datastax.com/products/datastax-enterprise
[core-features]: http://datastax.github.io/csharp-driver/features/
