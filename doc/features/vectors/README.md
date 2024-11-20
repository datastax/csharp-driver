# Vector support

## Native CQL `vector` type

Introduced in Cassandra 5.0, DSE 6.9 and Datastax Astra, a `vector` is represented as a [CqlVector&lt;T&gt;][cqlvector-api].

The `vector` type is handled by the driver the same way as any other CQL type. You can use 

## The `CqlVector<T>` C# type

The [API documentation][cqlvector-api] for this class contains useful information. Here's some examples:

### Creating vectors

```csharp
// these 2 are equivalent
var vector = new CqlVector<int>(1, 2, 3);
var vector = CqlVector<int>.New(new int[] { 1, 2, 3 });

// CqlVector<int>.New requires an array but you prefer using other types such as List 
// you can call the IEnumerable extension method .ToArray() - note that it performs a copy
var vector = CqlVector<int>.New(new List<int> { 1, 2, 3 }.ToArray());

// create a vector with the specified number of dimensions (this is similar to creating an array - new int[dimensions])
var vector = CqlVector<int>.New(3);

// Converting an array to a CqlVector without copying
var vector = new int[] { 1, 2, 3 }.AsCqlVector();

// Converting an IEnumerable to a CqlVector (calls .ToArray() internally so it performs a copy)
var vector = new int[] { 1, 2, 3 }.ToCqlVector();
```

### Modifying vectors

```csharp
var vector = CqlVector<int>.New(3);

// you can use the index operator just as if you were dealing with an array or list
vector[0] = 1; 
vector[1] = 2;
vector[2] = 3;
```

### Equality

`Equals()` is defined in the `CqlVector<T>` class but keep in mind that it uses `Array.SequenceEqual` internally which doesn't account for nested arrays/collections so `Equals()` will not work correctly for those cases.

```csharp
var vector1 = new CqlVector<int>(1, 2, 3);
var vector2 = new CqlVector<int>(1, 2, 3);
vector1.Equals(vector2); // this returns true
```

## Writing vector data and performing vector search operations

The `vector` type is handled by the driver the same way as any other CQL type.

The following examples use this schema. In this case, `j` is a 3 dimensional `vector` column of `float` values. Both the vector subtype and the number of dimensions can be changed. Any CQL type is valid as a vector subtype.

```sql
CREATE TABLE IF NOT EXISTS table1 (
     i int PRIMARY KEY, 
     j vector<float, 3>
);

/* Supported by C* 5.0, for vector search with the ANN operator */
CREATE CUSTOM INDEX IF NOT EXISTS ann_table1_index ON table1(j) USING 'StorageAttachedIndex';
```

### Simple Statements

```csharp
await session.ExecuteAsync(
    new SimpleStatement(
        "INSERT INTO table1 (i, j) VALUES (?, ?)", 
        1, 
        new CqlVector<float>(1.0f, 2.0f, 3.0f)));
var rowSet = await session.ExecuteAsync(
    new SimpleStatement(
        "SELECT * FROM table1 ORDER BY j ANN OF ? LIMIT ?", 
        new CqlVector<float>(0.6f, 0.5f, 0.9f),
        1));
var row = rowSet.Single();
var i = row.GetValue<int>("i");
var j = row.GetValue<CqlVector<float>?>("j");
```

### Prepared Statements

```csharp
var psInsert = await session.PrepareAsync("INSERT INTO table1 (i, j) VALUES (?, ?)");
var psSelect = await session.PrepareAsync("SELECT * FROM table1 ORDER BY j ANN OF ? LIMIT ?");

var boundInsert = psInsert.Bind(2, new CqlVector<float>(5.0f, 6.0f, 7.0f));
await session.ExecuteAsync(boundInsert);

var boundSelect = psSelect.Bind(new CqlVector<float>(4.7f, 5.0f, 5.0f), 1);
var rowSet = await session.ExecuteAsync(boundSelect);

var row = rowSet.Single();
var i = row.GetValue<int>("i");
var j = row.GetValue<CqlVector<float>>("j");
```

### LINQ and Mapper

The LINQ component of the driver doesn't support the `ANN` operator so it's probably best to avoid using LINQ when working with vectors. If a particular workload doesn't require the `ANN` operator then LINQ can be used without issues.

```csharp
// you can also provide a MappingConfiguration object to the Table/Mapper constructors 
// (or use MappingConfiguration.Global) programatically instead of these attributes
[Cassandra.Mapping.Attributes.Table("table1")]
public class Table1
{
    [Cassandra.Mapping.Attributes.PartitionKey]
    [Cassandra.Mapping.Attributes.Column("i")]
    public int I { get; set; }

    [Cassandra.Mapping.Attributes.Column("j")]
    public CqlVector<float>? J { get; set; }
}

// LINQ

var table = new Table<TestTable1>(session);
await table
    .Insert(new TestTable1 { I = 3, J = new CqlVector<float>(10.1f, 10.2f, 10.3f) })
    .ExecuteAsync();

// Using AllowFiltering is not recommended due to unpredictable performance. 
// Here we use AllowFiltering because the example schema is meant to showcase vector search
// but the ANN operator is not supported in LINQ yet.
var entity = (await table.Where(t => t.I == 3 && t.J == CqlVector<float>.New(new [] {10.1f, 10.2f, 10.3f})).AllowFiltering().ExecuteAsync()).SingleOrDefault(); 

// Alternative select using Query syntax instead of Method syntax
var entity = (await (
    from t in table 
    where t.J == CqlVector<float>.New(new [] {10.1f, 10.2f, 10.3f}) 
    select t
    ).AllowFiltering().ExecuteAsync()).SingleOrDefault();

// Mapper

var mapper = new Mapper(session);
await mapper.InsertAsync(
    new TestTable1 { I = 4, J = new CqlVector<float>(11.1f, 11.2f, 11.3f) });
var vectorSearchData = await mapper.FetchAsync<TestTable1>(
    "ORDER BY j ANN OF ? LIMIT ?", 
    new CqlVector<float>(10.9f, 10.9f, 10.9f), 
    1);
var entity = vectorSearchData.SingleOrDefault();
```


[cqlvector-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.CqlVector-1.html
