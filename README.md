# C# Driver Extensions for DataStax Enterprise

This driver is built on top of [C# CQL driver for Apache Cassandra][cassandra-driver] and provides the following
extensions for DataStax Enterprise:

* Serializers for geospatial types which integrate seamlessly with the driver.
* DSE graph integration.
* `IAuthenticator` implementations that use the authentication scheme negotiation in the server-side `DseAuthenticator`.

## Installation

The driver is distributed as a compressed zip file with the following structure:

- `README`: this file;
- `DseDriver<version>.nupkg`: Nuget package.
- `apidocs/*`: API reference.

To make the Nuget package available to other projects, you can [add it to your Nuget feeds][nuget-self-hosting].

## Getting Started

`DseCluster` and `DseSession` wrap their CQL driver counterparts. All CQL features are available (see the 
[CQL driver manual][core-manual]), so you can use a `IDseSession` in lieu of a `ISession`:

```csharp
using Dse;

DseCluster cluster = DseCluster.Builder()
                               .AddContactPoint("127.0.0.1")
                               .Build();
IDseSession session = cluster.Connect();
Row row = session.Execute("select * from system.local").First();
Console.WriteLine(row.GetValue<string>("cluster_name"));
//...
//Dispose on app shutdown
cluster.Dispose();
```

## Authentication

For clients connecting to a DSE cluster secured with `DseAuthenticator`, two authentication providers are included:

* `DsePlainTextAuthProvider`: plain-text authentication;
* `DseGSSAPIAuthProvider`: GSSAPI authentication.

To configure a provider, pass it when initializing the cluster:

```csharp
using Dse;
using Dse.Auth;

DseCluster dseCluster = DseCluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithAuthProvider(new DseGssapiAuthProvider())
    .Build();
```

See the API docs of each implementation for more details.


## Geospatial types

DSE 5 comes with a set of additional types to represent geospatial data: `PointType`, `LineStringType`, `PolygonType`
and `CircleType`:

```
cqlsh> CREATE TABLE points_of_interest(name text PRIMARY KEY, coords 'PointType');
cqlsh> INSERT INTO points_of_interest (name, coords) VALUES ('Eiffel Tower', 'POINT(48.8582 2.2945)');
```

The DSE driver includes C# representations of these types, that can be used directly in queries:

```csharp
using Dse.Geometry;

Row row = session.Execute("SELECT coords FROM points_of_interest WHERE name = 'Eiffel Tower'").First();
Point coords = row.GetValue<Point>("coords");

var statement = new SimpleStatement("INSERT INTO points_of_interest (name, coords) VALUES (?, ?)",
    "Washington Monument", 
    new Point(38.8895, 77.0352));
session.Execute(statement);
```

## Graph

`IDseSession` has dedicated methods to execute graph queries:

```csharp
using Dse.Graph;

session.ExecuteGraph("system.createGraph('demo').ifNotExist().build()");

GraphStatement s1 = new SimpleGraphStatement("g.addV(label, 'test_vertex')").SetGraphName("demo");
session.ExecuteGraph(s1);

GraphStatement s2 = new SimpleGraphStatement("g.V()").SetGraphName("demo");
GraphResultSet rs = session.ExecuteGraph(s2);
Vertex vertex = rs.First();
Console.WriteLine(vertex.Label);
```

### Graph options

You can set default graph options when initializing the cluster. They will be used for all graph statements. For
example, to avoid repeating `SetGraphName("demo")` on each statement:

```csharp
DseCluster dseCluster = DseCluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithGraphOptions(new GraphOptions().SetName("demo"))
    .Build();
```

If an option is set manually on a `GraphStatement`, it always takes precedence; otherwise the default option is used.
This might be a problem if a default graph name is set, but you explicitly want to execute a statement targeting
`system`, for which no graph name must be set. In that situation, use `GraphStatement#SetSystemQuery()`:

```csharp
GraphStatement s = new SimpleGraphStatement("system.createGraph('demo').ifNotExist().build()")
    .SetSystemQuery();
session.ExecuteGraph(s);
```

### Query execution

As explained, graph statements can be executed with the session's `ExecuteGraph` method. There is also an
asynchronous equivalent called `ExecuteGraphAsync`.

### Handling results

Graph queries return a `GraphResultSet`, which is essentially an enumerable of `GraphResult`:

```csharp
GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));

// Iterating as GraphResult:
foreach (GraphResult r in rs)
{
    Console.WriteLine(r);
}
```

`GraphResult` wraps the JSON responses returned by the server. You can cast the result to a specific type as it
implements implicit conversion operators to `Vertex` and `Edge`:

```csharp
GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));

// Iterating as GraphResult:
foreach (Vertex vextex in rs)
{
    Console.WriteLine(vertex.Label);
}
```

`GraphResult` also provides conversion methods for scalar values like `ToDouble()`, `ToInt32()` and `ToString()`:

```csharp
GraphResult r = session.ExecuteGraph(new SimpleGraphStatement("g.V().count()")).First();
Console.WriteLine("The graph has {0} vertices.", r.ToInt32());
```

`GraphResult` inherits from [`DynamicObject`][dynamic], allowing you to consume it using the dynamic keyword and as a
dictionary. 

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

Parameters can have the following types:

* `null`
* `bool`, `int`, `double`, `float`, `short` or `string`
* `Array`, `IEnumerable`, `IDictionary` instances

### Prepared statements

Prepared graph statements are not supported by DSE yet (they will be added in the near future).

[cassandra-driver]: https://github.com/datastax/csharp-driver
[core-manual]: http://docs.datastax.com/en//developer/csharp-driver/3.0/csharp-driver/whatsNew2.html
[modern]: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure
[nuget-self-hosting]: http://docs.nuget.org/create/hosting-your-own-nuget-feeds
[dynamic]: https://msdn.microsoft.com/en-us/library/dd264736.aspx