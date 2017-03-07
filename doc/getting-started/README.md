*This is the documentation for the DataStax Enterprise C# Driver for [DSE][dse]. This driver is built on top of the
[DataStax C# driver for Apache Cassandra][core-driver] and enhanced for the adaptive data management and mixed
workload capabilities provided by [DataStax Enterprise][dse]. Therefore a lot of the underlying concepts are the same
and to keep this documentation focused we will be linking to the relevant sections of the [DataStax C# driver
for Apache Cassandra][core-driver-docs] documentation where necessary.*

# Getting started

Getting started with the DataStax Enterprise C# Driver.

## Installation

[Get it on Nuget][nuget]

```
PM> Install-Package Dse
```

## Upgrading from the core driver

To Upgrade from `CassandraCSharpDriver` to `Dse`, add the namespace with the `using` directive.

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
[core-driver]: https://github.com/datastax/csharp-driver-dse
[core-driver-docs]: http://datastax.github.io/csharp-driver/
[core-features]: http://datastax.github.io/csharp-driver/features/
