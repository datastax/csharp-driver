# Mapper component

The Mapper component handles the mapping of CQL table columns to fields in your classes.

The Mapper component (previously known as [cqlpoco][cqlpoco]) is a lightweight object mapper for Apache Cassandra and DataStax Enterprise. It lets you write queries with CQL, while it takes care of mapping rows returned from the server to your classes. It was inspired by [PetaPoco][petapoco], NPoco, [Dapper.NET][dapper] and the [cqlengine][cqlengine] project.

To use the Mapper:

1.- Add the following using statement to your class:

```csharp
using Cassandra.Mapping;
```

2.- Retrieve an `ISession` instance in the usual way and reuse that session within all the classes in your client application.

3.- Instantiate a `Mapper` object using its constructor:

```csharp
IMapper mapper = new Mapper(session);
```

New `Mapper` instances can be created each time they are needed, as short-lived instances, as long as you are reusing the same `ISession` and `MappingConfiguration` instances. `MappingConfiguration.Global` is used if you don't provide one.

In some scenarios, it might make sense to create multiple `MappingConfiguration` instances and create one mapper instance per `MappingConfiguration` (e.g. reuse the same POCOs for multiple keyspaces).
See [this example](https://github.com/datastax/csharp-driver/blob/master/examples/Mapper/MultipleKeyspacesSingleSession/MapperManager.cs) for details.

The Mapper works by mapping the column names in your CQL statement to the property names on your classes.

For example:

```csharp
public class User
{
   public Guid UserId { get; set; }
   public string Name { get; set; }
}

// Get a list of users from Cassandra/DSE
IEnumerable<User> users = mapper.Fetch<User>("SELECT userid, name FROM users");
IEnumerable<User> users = mapper.Fetch<User>("SELECT * FROM users WHERE name = ?", someName);
```

Simple scenarios such as this are possible without doing any further mapping configuration. When using parameters, use query markers (`?`) instead of hardcoded stringified values, this improves serialization performance and lower memory consumption.

The Mapper will create new instances of your classes using the parameter-less constructor.

## Configuring mappings

In many scenarios, you need more control over how your class maps to a CQL table. You have two ways of configuring the Mapper:

- decorate your classes with attributes
- define mappings in code using the fluent interface

An example using the fluent interface:

```csharp
MappingConfiguration.Global.Define(
   new Map<User>()
      .TableName("users")
      .PartitionKey(u => u.UserId)
      .Column(u => u.UserId, cm => cm.WithName("id")));
```

You can also create a class to group all your mapping definitions.

```csharp
public class MyMappings : Mappings
{
   public MyMappings()
   {
       // Define mappings in the constructor of your class
       // that inherits from Mappings
       For<User>()
          .TableName("users")
          .PartitionKey(u => u.UserId)
          .Column(u => u.UserId, cm => cm.WithName("id")));
       For<Comment>()
          .TableName("comments");
   }
}
```

Then, you can assign the mappings class in your configuration.

```csharp
MappingConfiguration.Global.Define<MyMappings>();
```

You should map one C# class per table. The Mapper component of the driver will use the configuration defined when creating the `Mapper` instance to determine to which keyspace and table it maps to, using `MappingConfiguration.Global` when not specified.

## Mapper API example

A simple query example is great, but the Mapper has many other methods for doing things like Inserts, Updates, Deletes, selecting a single record and more. And all methods have async counterparts. Here's a quick sampling.

```csharp
// All query methods (Fetch, Single, First, etc.) will auto generate
// the SELECT and FROM clauses if not specified.
IEnumerable<User> users = mapper.Fetch<User>();
IEnumerable<User> users = mapper.Fetch<User>("FROM users WHERE name = ?", someName);
IEnumerable<User> users = mapper.Fetch<User>("WHERE name = ?", someName);

// Single and SingleOrDefault for getting a single record
var user = mapper.Single<User>("WHERE userid = ?", userId);
var user = mapper.SingleOrDefault<User>("WHERE userid = ?", userId);

// First and FirstOrDefault for getting first record
var user = mapper.First<User>("SELECT * FROM users");
var user = mapper.FirstOrDefault<User>("SELECT * FROM users");

// All query methods also support "flattening" to just the column's value type when
// selecting a single column 
Guid userId = mapper.First<Guid>("SELECT userid FROM users");
IEnumerable<string> names = mapper.Fetch<string>("SELECT name FROM users");

// Insert a POCO var newUser = new User { UserId = Guid.NewGuid(), Name = "SomeNewUser" };
mapper.Insert(newUser);

// Update with POCO someUser.Name = "A new name!";
mapper.Update(someUser);

// Update with CQL (will prepend table name to CQL)
mapper.Update<User>("SET name = ? WHERE id = ?", someNewName, userId);

// Delete with POCO 
mapper.Delete(someUser);

// Delete with CQL (will prepend table name to CQL)
mapper.Delete<User>("WHERE id = ?", userId);
```

[cqlpoco]: https://github.com/LukeTillman/cqlpoco
[dapper]: https://github.com/StackExchange/dapper-dot-net
[petapoco]: https://github.com/toptensoftware/PetaPoco
[cqlengine]: https://github.com/cqlengine/cqlengine