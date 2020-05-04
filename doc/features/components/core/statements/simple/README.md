# Simple statements

Use `SimpleStatement` for queries that will be executed only once (or just a few times):

```csharp
var statement =
    new SimpleStatement(
        "SELECT value FROM application_params WHERE name = 'greeting_message'");
session.Execute(statement);
```

Each time you execute a simple statement, Cassandra parses the query string again; nothing is cached (neither on the client nor on the server):

```ditaa
client                             driver                Cassandra
--+----------------------------------+---------------------+------
  |                                  |                     |
  | Session.Execute(SimpleStatement) |                     |
  |--------------------------------->|                     |
  |                                  | QUERY(query_string) |
  |                                  |-------------------->|
  |                                  |                     |
  |                                  |                     |
  |                                  |                     | - parse query string
  |                                  |                     | - execute query
  |                                  |                     |
  |                                  |       ROWS          |
  |                                  |<--------------------|
  |                                  |                     |
  |<---------------------------------|                     |
```

If you execute the same query often (or a similar query with different column values), consider a
[prepared statement](../prepared/) instead.

## Using values

Instead of hard-coding everything in the query string, you can use bind markers and provide values
separately:

* by position:

    ```csharp
    new SimpleStatement(
      "SELECT value FROM application_params WHERE name = ?", 
      "greeting_message");
    ```

* by name:

    ```csharp
    var values = new Dictionary<string, object>
    {
        {"n", "greeting_message"}
    };
    session.Execute(
      new SimpleStatement(
        values,
        "SELECT value FROM application_params WHERE name = :n"));
    ```

This syntax has a few advantages:

* if the values come from some other part of your code, it is safer and looks cleaner than doing the concatenation yourself;
* you don't need to translate the values to their string representation. The driver will send them alongside the query, in their serialized binary form.

The number of values must match the number of placeholders in the query string, and their types must match the database schema. Note that the driver does not parse simple statements, so it cannot perform those checks on the client side; if you make a mistake, the query will be sent anyway, and the server will reply with an error, that gets translated into a driver exception.

### Type inference

Another consequence of not parsing query strings is that the driver has to guess how to serialize values, based on their .NET type (see the [default type mappings](../../../datatypes)).
This can be tricky, in particular for numeric types:

```csharp
// schema: create table bigints(b bigint primary key)
session.Execute(
    new SimpleStatement(
      "INSERT INTO bigints (b) VALUES (?)",
      1));
// InvalidQueryException
```

The problem here is that the literal `1` has the .NET type `int`. So the driver serializes it as a CQL `int` (4 bytes), but the server expects a CQL `bigint` (8 bytes). The fix is to specify the correct .NET type:

```csharp
// schema: create table bigints(b bigint primary key)
session.Execute(
    new SimpleStatement(
      "INSERT INTO bigints (b) VALUES (?)",
      1L)); // long literal
```

You could also use [prepared statements](../prepared/), which don't have this limitation since parameter types are known in advance.
