# ADO.NET

Implementation of the ADO.NET interfaces and common abstract classes present in the `System.Data` namespace of the 
.NET Framework: `IDbConnection`, `IDbCommand`, and `IDbDataAdapter`.

It allows users to interact with a Cassandra cluster using a common .NET data access pattern.

ADO.NET design limits how you can interact with Cassandra clusters (sync only, open / close pattern), for that reason
**it is recommended that you use the [Core component](../core) of the driver instead**.

## Example

```csharp
var connection  = new CqlConnection(connectionString);
connection.Open();
try
{
   var command = connection.CreateCommand();
   command.CommandText = "UPDATE tbl SET val = 'z' WHERE id = 1";
   command.ExecuteNonQuery();
}
finally
{
   connection.Close();
}
```