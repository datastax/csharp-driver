# User-defined functions and aggregates

Cassandra 2.2 introduced user-defined functions (UDF) and aggregates support.

You access function and aggregate values in your queries like regular columns:

```csharp
session.Execute("SELECT avg(salary) as salary FROM employees");
```

The driver also exposes UDFs and aggregates metadata information, for example to retrieve the metadata information of
a UDF named `iif`, that takes a `boolean` and `int` parameter.

```csharp
FunctionMetadata udf = cluster.Metadata.GetFunction("ks1", "iif", new []{"boolean", "int"});
```