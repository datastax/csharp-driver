# DataStax Enterprise C# Driver

This driver is built on top of [DataStax C# driver for Apache Cassandra][cassandra-driver] and provides the following
additions for [DataStax Enterprise][dse]:

* `IAuthenticator` implementations that use the authentication scheme negotiation in the server-side
`DseAuthenticator`.
* [DSE graph][dse-graph] integration.
* Serializers for geospatial types which integrate seamlessly with the driver.

The driver supports .NET Framework 4.5+ and .NET Core 1+.

The DataStax Enterprise C# Driver can be used solely with DataStax Enterprise. Please consult
[the license](#license).


## Installation

[Get it on Nuget][nuget]

```
PM> Install-Package Dse
```

[![Build status](https://travis-ci.org/datastax/csharp-dse-driver.svg?branch=master)](https://travis-ci.org/datastax/csharp-dse-driver)
[![Windows Build status](https://ci.appveyor.com/api/projects/status/yuk0p8i8r2l9f6xk/branch/master?svg=true)](https://ci.appveyor.com/project/DataStax/csharp-driver-dse/branch/master)
[![Latest stable on Nuget.org](https://img.shields.io/nuget/v/Dse.svg)](https://www.nuget.org/packages/Dse)

## Documentation

- [Documentation index][doc-index]
- [API docs][api-docs]
- [FAQ][faq]

## Getting Help

You can use the [project mailing list][mailing-list] or create a ticket on the [Jira issue tracker][jira].

## Getting Started

`IDseCluster` and `IDseSession` extend their CQL driver counterparts, so you can use `Dse` instances to execute CQL
queries.

```csharp
using Dse;
```

```csharp
IDseCluster cluster = DseCluster.Builder()
                                .AddContactPoint("127.0.0.1")
                                .Build();
IDseSession session = cluster.Connect();
Row row = session.Execute("select * from system.local").First();
Console.WriteLine(row.GetValue<string>("cluster_name"));
```

## Authentication

For clients connecting to a DSE cluster secured with `DseAuthenticator`, two authentication providers are included:

* `DsePlainTextAuthProvider`: plain-text authentication;
* `DseGssapiAuthProvider`: GSSAPI authentication.

To configure a provider, pass it when initializing the cluster:

```csharp
using Dse;
using Dse.Auth;
```

```csharp
IDseCluster dseCluster = DseCluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithAuthProvider(new DseGssapiAuthProvider())
    .Build();
```

## Graph

`IDseSession` has dedicated methods to execute graph queries:

```csharp
using Dse.Graph;
```

```csharp
session.ExecuteGraph("system.createGraph('demo').ifNotExist().build()");

GraphStatement s1 = new SimpleGraphStatement("g.addV(label, 'test_vertex')").SetGraphName("demo");
session.ExecuteGraph(s1);

GraphStatement s2 = new SimpleGraphStatement("g.V()").SetGraphName("demo");
GraphResultSet rs = session.ExecuteGraph(s2);

IVertex vertex = rs.First().To<IVertex>();
Console.WriteLine(vertex.Label);
```

### Graph options

You can set default graph options when initializing the cluster. They will be used for all graph statements. For
example, to avoid repeating `SetGraphName("demo")` on each statement:

```csharp
IDseCluster dseCluster = DseCluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithGraphOptions(new GraphOptions().SetName("demo"))
    .Build();
```

If an option is set manually on a `GraphStatement`, it always takes precedence; otherwise the default option is used.
This might be a problem if a default graph name is set, but you explicitly want to execute a statement targeting
`system`, for which no graph name must be set. In that situation, use `GraphStatement.SetSystemQuery()`:

```csharp
GraphStatement s = new SimpleGraphStatement("system.createGraph('demo').ifNotExist().build()")
    .SetSystemQuery();
session.ExecuteGraph(s);
```

### Query execution

As explained, graph statements can be executed with the session's `ExecuteGraph` method. There is also an
asynchronous equivalent called `ExecuteGraphAsync` that returns a Task that can be awaited upon.

### Handling results

Graph queries return a `GraphResultSet`, which is a sequence of `GraphNode` elements:

```csharp
GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));

// Iterating as IGraphNode
foreach (IGraphNode r in rs)
{
    Console.WriteLine(r);
}
```

`IGraphNode` represents a response item returned by the server. Each item can be converted to the expected type:
                                                              
```csharp
GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
IVertex vertex = rs.First().To<IVertex>();
Console.WriteLine(vertex.Label);
```

Additionally, you can apply the conversion to all the sequence by using `GraphResultSet.To<T>()` method:

```csharp
foreach (IVertex vertex in rs.To<IVertex>())
{
    Console.WriteLine(vertex.Label);
}
```

`GraphNode` provides [implicit conversion operators][implicit] to `string`, `int`, `long` and others in order to 
improve code readability, allowing the following C# syntax:

```csharp
var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko').values('location')"));
foreach (string location in rs)
{
    Console.WriteLine(location);
}
```

`GraphNode` inherits from [`DynamicObject`][dynamic], allowing you to consume it using the `dynamic` keyword and/or
as a dictionary. 

```csharp
dynamic r = session.ExecuteGraph(new SimpleGraphStatement("g.V()")).First();
```

### Parameters

Graph query parameters are always named. Parameter bindings are passed as an anonymous type or as a
`IDictionary<string, object>` alongside the query:

```csharp
session.ExecuteGraph("g.addV(label, vertexLabel)", new { vertexLabel = "test_vertex_2" });
```

Note that, unlike in CQL, Gremlin placeholders are not prefixed with ":".

### Prepared statements

Prepared graph statements are not supported by DSE Graph yet (they will be added in the near future).

## Geospatial types

DSE 5 comes with a set of additional types to represent geospatial data: `PointType`, `LineStringType` and
`PolygonType`:

```
cqlsh> CREATE TABLE points_of_interest(name text PRIMARY KEY, coords 'PointType');
cqlsh> INSERT INTO points_of_interest (name, coords) VALUES ('Eiffel Tower', 'POINT(48.8582 2.2945)');
```

The DSE driver includes C# representations of these types, that can be used directly in queries:

```csharp
using Dse.Geometry;
```

```csharp
Row row = session.Execute("SELECT coords FROM points_of_interest WHERE name = 'Eiffel Tower'").First();
Point coords = row.GetValue<Point>("coords");

var statement = new SimpleStatement("INSERT INTO points_of_interest (name, coords) VALUES (?, ?)",
    "Washington Monument", 
    new Point(38.8895, 77.0352));
session.Execute(statement);
```

## Compatibility

- DataStax Enterprise versions 4.5 and above.
- .NET Framework versions 4.5 and above and .NET Core versions 1.0 and above.

Note: DataStax products do not support big-endian systems.

## License

Copyright 2016-2017 DataStax

http://www.datastax.com/terms/datastax-dse-driver-license-terms

[dse]: http://www.datastax.com/products/datastax-enterprise
[dse-graph]: http://www.datastax.com/products/datastax-enterprise-graph
[cassandra-driver]: https://github.com/datastax/csharp-driver
[core-driver-docs]: http://docs.datastax.com/en/developer/csharp-driver-dse/latest/
[modern]: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure
[nuget]: https://nuget.org/packages/Dse/
[dynamic]: https://msdn.microsoft.com/en-us/library/dd264736.aspx
[jira]: https://datastax-oss.atlassian.net/projects/CSHARP/issues
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user
[doc-index]: http://docs.datastax.com/en/developer/csharp-driver-dse/latest/
[api-docs]: http://docs.datastax.com/en/drivers/csharp-dse/2.0/
[faq]: http://docs.datastax.com/en/developer/csharp-driver-dse/latest/faq/
[implicit]: https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/implicit