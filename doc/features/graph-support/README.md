# Graph support

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
Vertex vertex = rs.First();
Console.WriteLine(vertex.Label);
```

## Graph options

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

## Asynchronous query execution

Graph statements can also be executed without blocking the calling thread using `ExecuteGraphAsync()` method.

```csharp
GraphResultSet rs = await session.ExecuteGraphAsync(new SimpleGraphStatement("g.V()"));
```

## Handling results

Graph queries return a `GraphResultSet`, which is essentially an enumerable of `GraphNode`:

```csharp
GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));

// Iterating as GraphNode
foreach (GraphNode n in rs)
{
    Console.WriteLine(n);
}
```

`GraphNode` represent a response item returned by the server. You can cast the result to a specific type as it
implements implicit conversion operators to `Vertex`, `Edge` and `Path`:

```csharp
GraphResultSet rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));

// Iterating as Vertex
foreach (Vertex vextex in rs)
{
    Console.WriteLine(vertex.Label);
}
```

`GraphNode` also provides conversion methods for scalar values like `ToDouble()`, `ToInt32()`, `To<T>()` and
`ToString()`:

```csharp
GraphNode r = session.ExecuteGraph(new SimpleGraphStatement("g.V().count()")).First();
Console.WriteLine("The graph has {0} vertices.", r.ToInt32());
```

`GraphNode` inherits from [`DynamicObject`][dynamic], allowing you to consume it using the `dynamic` keyword and/or as a
dictionary. 

```csharp
dynamic r = session.ExecuteGraph(new SimpleGraphStatement("g.V()")).First();
```

## Parameters

Graph query parameters are always named. Parameter bindings are passed as an anonymous type or as a
`IDictionary<string, object>` alongside the query:

```csharp
session.ExecuteGraph("g.addV(label, vertexLabel)", new { vertexLabel = "test_vertex_2" });
```

Note that, unlike in CQL, Gremlin placeholders are not prefixed with ":".

## Prepared statements

Prepared graph statements are not supported by DSE Graph yet (they will be added in the near future).

[modern-graph]: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure
[dynamic]: https://msdn.microsoft.com/en-us/library/dd264736.aspx