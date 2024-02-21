# Nulls and unset

## Null and unset values

To complete a distributed delete operation, Cassandra replaces it with a special value called a [tombstone][tombstone] which can be propagated to replicas. When inserting or updating a field, Cassandra allows you to set a certain field to `null` as a way to clear the value of a field and it is considered a delete operation.

In some cases, you might be inserting rows using `null` for values that are not specified, and even though our intention is to leave the value empty, Cassandra represents it as a tombstone causing an unnecessary overhead.

To avoid tombstones, in previous versions of Cassandra, you can use different query combinations only containing the fields that have a value.

## Unset

Cassandra 2.2 introduced the concept of "unset" for a parameter value. At server level, this field value is not considered. This can be represented in the C# driver with the class [Unset][unset-api].

```csharp
// Prepare once in your application lifetime
var ps = session.Prepare("INSERT INTO tbl1 (id, val1) VALUES (?, ?)");

// Bind the unset value in a prepared statement
session.Execute(ps.Bind(id, Unset.Value));
```

[tombstone]: http://docs.datastax.com/en/glossary/doc/glossary/gloss_tombstone.html
[unset-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.Unset.html
