# Routing queries

When using the `TokenAwarePolicy`, the driver uses the `RoutingKey` to determine which nodes is used as coordinator for a given statement.

Note that the internal `TokenMap` must be up to date in order for this feature to work correctly. If metadata synchronization is enabled (which it is by default), then the driver will automatically keep it up to date. For more information on metadata synchronization, [check out this page](../metadata).

## Prepared statements 

When using prepared statements, the driver will determine which of the query parameters compose the partition
key based on the prepared statement metadata.

Consider a table users that has a single partition key, id.

```csharp
PreparedStatement prepared = session.Prepare(
      "INSERT INTO users (id, name) VALUES (?, ?)");
```

When binding the parameters, the driver knows which parameter corresponds to the partition key.

```csharp
BoundStatement bound = prepared.Bind(Guid.NewGuid(), "Franz Ferdinand");
session.Execute(bound);
```

As a rule of thumb, **use prepared statements and the driver does all the routing for you**.

## Simple statements 

If you need to use a simple statement with routing, you must specify the routing values.

```csharp
var id = Guid.NewGuid();
var query = new SimpleStatement(
      "INSERT INTO users (id, name) VALUES (?, ?)", id, "Franz Ferdinand");
// You must specify the values that compose the partition key
query.SetRoutingValues(id);
session.Execute(query);
```

## Batch statements 

If you want to enable routing for batch statements, you must specify the routing values.

```csharp
var partitionKey = Guid.NewGuid();
var batch = new Batch();
// ... Add statements to the query
// You must specify the values that compose the partition key
batch.SetRoutingValues(partitionKey);
session.Execute(batch);
```