# Graph support

The driver provides support for DataStax Graph. The `ISession` interface has dedicated methods to execute graph queries with the [Gremlin] graph traversal language.

*This manual only covers driver usage; for more information about server-side configuration and data modeling, refer to the [DSE developer guide].*

```csharp
using Cassandra.DataStax.Graph;
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

## DataStax Graph and the Core Engine (DSE 6.8+)

Graphs that use the core engine (default in DSE 6.8+) only support GraphSON3. The driver automatically switches the default GraphSON version if it detects the core engine.

However, this graph engine detection is based on the metadata. `In some scenarios you might experience some server errors where the message mentions that the GraphSON version is incompatible.` This happens when the graph has been newly created and is not yet in the metadata. It also happens when you don't provide the graph name.

If you are running into these errors you can set the GraphSON version explicitly:

```csharp
// with the legacy configuration method
var cluster =
    Cluster.Builder()
           .AddContactPoint("127.0.0.1")
           .WithGraphOptions(
               new GraphOptions().SetGraphProtocolVersion(GraphProtocol.GraphSON3))
           .Build();

// with execution profiles
var cluster =
    Cluster.Builder()
           .AddContactPoint("127.0.0.1")
           .WithExecutionProfiles(opt => opt
                .WithProfile("default", profile => profile
                    .WithGraphOptions(
                        new GraphOptions().SetGraphProtocolVersion(GraphProtocol.GraphSON3))))
           .Build();
```

## Query Execution APIs

There are 2 APIs that you can use to execute graph queries:

- A `fluent`, builder-like API;
- A Gremlin traversal **string** execution API.

With the Gremlin traversal **string** execution API, you pass a Gremlin traversal directly in a plain C# string via `SimpleGraphStatement` objects. Every example on this page uses this API.

To use the `fluent` API you must add an additional dependency to your application: the [DataStax C# Graph Extension]. This extension pulls in the `Gremlin.Net` nuget package which is the Apache Tinkerpop GLV (Gremlin Language Variant) for the .NET platform.

For examples and more information on the `fluent` API, please take a look at the documentation for the [DataStax C# Graph Extension].

If your application just uses the Gremlin traversal string execution API, then it is not necessary to install any other packages besides the core driver, i.e., `CassandraCSharpDriver`.

## Graph options

You can set default graph options when initializing the cluster. They will be used for all graph statements. For example, to avoid repeating `SetGraphName("demo")` on each statement:

```csharp
// with the legacy configuration method
ICluster cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithGraphOptions(new GraphOptions().SetName("demo"))
    .Build();

// with execution profiles
ICluster cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithExecutionProfiles(opt => opt
        .WithProfile("default", profile => profile
            .WithGraphOptions(new GraphOptions().SetName("demo"))))
    .Build();
```

For more information on execution profiles, see [this page of the manual](../execution-profiles).

If an option is set manually on a `GraphStatement`, it always takes precedence; otherwise the default option is used. This might be a problem if a default graph name is set, but you explicitly want to execute a statement targeting `system`, for which no graph name must be set. In that situation, use `GraphStatement.SetSystemQuery()`:

```csharp
GraphStatement s = 
    new SimpleGraphStatement("system.createGraph('demo').ifNotExist().build()").SetSystemQuery();

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

`GraphNode` provides [implicit conversion operators][implicit] to `string`, `int`, `long` and others in order to improve code readability, allowing the following C# syntax:

```csharp
var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko').values('location')"));
foreach (string location in rs)
{
    Console.WriteLine(location);
}
```

`GraphNode` inherits from [`DynamicObject`][dynamic], allowing you to consume it using the `dynamic` keyword and/or as a dictionary. 

```csharp
dynamic r = session.ExecuteGraph(new SimpleGraphStatement("g.V()")).First();
```

You can get the GraphNode properties using the `GetProperty()` method from the element:

```csharp
var vertex = session.ExecuteGraph(new SimpleGraphStatement("g.V()")).First().To<IVertex>();
// Assuming that vertex with properties: name:string, and age:int
Console.WriteLine(vertex.GetProperty("name").Value.ToString());
Console.WriteLine(vertex.GetProperty("age").Value.ToInt32());
```

## Parameters

Graph query parameters are always named. Parameter bindings are passed as an anonymous type or as a `IDictionary<string, object>` alongside the query:

```csharp
session.ExecuteGraph("g.addV(label, vertexLabel)", new { vertexLabel = "test_vertex_2" });
```

Note that, unlike in CQL, Gremlin placeholders are not prefixed with ":".

## Graph types

The DataStax C# driver supports a wide variety of TinkerPop types and [DSE types](../datatypes/).

For example:

```csharp
var traversal = new SimpleGraphStatement(
    "g.addV('sample').property('uid', uid).property('ip_address', address)",
    new { uid = Guid.NewGuid(), address = IPAddress.Parse("127.0.0.1") });

session.ExecuteGraph(traversal);
```

The same types are also supported for traversal execution results:

```csharp
var rs = session.ExecuteGraph("g.V().hasLabel('sample').values('ip_address')");
foreach (IPAddress ip in rs.To<IPAddress>())
{  
    Console.WriteLine(ip.ToString());
}
```

### User-defined types

User-defined types (UDTs) in DataStax Graph are supported with the C# driver.

In order to use them you need to map them to .NET types in the same way as you would use UDTs in standard CQL workloads with Apache Cassandra. You can find documentation on how to map UDTs to .NET types in [the UDT section of the driver manual](../udts).

Reading UDT values in traversal results does not require a UDT mapping as UDTs can be deserialized to `IDictionary<string, GraphNode>` but a UDT mapping is required if you need to provide a UDT value as a parameter.

### Without a UDT mapping

Here's an example of reading UDT values and their properties without a UDT mapping:

```csharp
var firstResult = session.ExecuteGraph(new SimpleGraphStatement(
    "g.V().hasLabel('users_contacts').has('id', 305).properties('contacts')")).ToArray()[0];

// the 'contacts' vertex property is a list<contact> where 'contact' is a UDT
var firstContact = firstElement.To<IVertexProperty>().Value.To<List<IDictionary<string, GraphNode>>>().First();

// 'emails' is a property of 'contact' and its type is list<text>
var firstContactEmails = firstContact["emails"].To<IEnumerable<string>>();
```

The example doesn't contain a vertex insertion with parameters because that requires a UDT mapping (see the next example).

Attempting to provide a `IDictionary<string, GraphNode>` value as a UDT parameter will result in a type mismatch server error.

## With a UDT mapping

This example contains two traversals:

- One that inserts a vertex with a UDT value as parameter for its property;
- One that selects all vertices with that label and retrieves the UDT property.

```csharp
class Contact
{
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public IEnumerable<string> Emails { get; set; }
}
```

```csharp
session.UserDefinedTypes.Define(
    UdtMap.For<Contact>(udtName: "contact", keyspace: "demo")
            .Map(c => c.FirstName, "first_name")
            .Map(c => c.LastName, "last_name")
            .Map(c => c.Emails, "emails"));

var john = new Contact
{
    FirstName = "John", LastName "Williams", Emails = new [] { "john@email.com" }
};

// the 'contacts' property is a list<contact> where 'contact' is a UDT
session.ExecuteGraph(new SimpleGraphStatement(
    "g.addV('users_contacts').property('id', 123).property('contacts', contacts)",
    new { contacts = new List<Contact> { john }});

var firstResult = session.ExecuteGraph(new SimpleGraphStatement(
    "g.V().hasLabel('users_contacts').has('id', 305).properties('contacts')")).ToArray()[0];

var firstContact = firstElement.To<IVertexProperty>().Value.To<List<Contact>>().First();

// 'Emails' is a property of the 'Contact' class and its type is IEnumerable<string>
var firstContactEmails = firstContact.Emails;
```

## Prepared statements

Prepared graph statements are not supported by DSE Graph.

[modern-graph]: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#_the_graph_structure
[dynamic]: https://msdn.microsoft.com/en-us/library/dd264736.aspx
[implicit]: https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/implicit
[DSE developer guide]: https://docs.datastax.com/en/dse/6.8/dse-dev/datastax_enterprise/graph/graphTOC.html
[Gremlin]: https://docs.datastax.com/en/dse/6.8/dse-dev/datastax_enterprise/graph/dseGraphAbout.html#WhatisGremlin?
[DataStax C# Graph Extension]: https://docs.datastax.com/en/developer/csharp-dse-graph/latest/