# Driver components

The driver contains four different components that you can choose to interact with the server nodes.

- [**Core component**](core): The core component is responsible for maintaining a pool of connections to the cluster and
executes the statements based on client configuration.
- [**Mapper component**](mapper): The Mapper component handles the mapping of CQL table columns to fields in your classes.
- [**Linq component**](linq): The Linq component of the driver is an implementation of Linq `IQueryProvider` and `IQueryable<T>`
 interfaces that allows you to write CQL queries in LINQ and read the results using your object model.
- [**ADO.NET**](adonet): Implementation of the ADO.NET interfaces and abstract classes common present in the `System.Data`
namespace of the .NET Framework: `IDbConnection`, `IDbCommand`, and `IDbDataAdapter`.
