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
```

`GraphResultSet` is a sequence of `GraphNode` elements. Each item can be converted to the expected type.

```csharp
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