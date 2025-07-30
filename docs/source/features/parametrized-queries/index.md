# Parameterized queries

You can bind the values of parameters in a `BoundStatement` or `SimpleStatement` either by position or by using
named markers.

## Positional parameters example

```csharp
var statement = session.Prepare("SELECT * FROM table where a = ? and b = ?");
// Bind parameter by marker position 
session.Execute(statement.Bind("aValue", "bValue"));
```

## Named parameters example

You can declare the named markers in your queries and use as parameter names when binding.

```csharp
var statement = session.Prepare("SELECT * FROM table where a = :a and b = :b"); 
// Bind by name using anonymous types 
session.Execute(statement.Bind( new { a = "aValue", b = "bValue" }));
```