# Linq component

The Linq component of the driver is an implementation of LINQ `IQueryProvider` and `IQueryable<T>` interfaces that allows you to write CQL queries in Linq and read the results using your object model.

When you execute a Linq statement, the component translates language-integrated queries into CQL and sends them to the cluster for execution. When the cluster returns the results, the LINQ component translates them back into objects that you can work with in C#.

Linq query execution involves expression evaluation which brings an additional overhead each time a Linq query is executed.

1.- Add a using statement to your class:

```csharp
using Cassandra.Data.Linq;
```

2.- Retrieve an `ISession` instance in the usual way and reuse that session within all the classes in your client application.

3.- You can get an IQueryable instance of using the Table constructor:

```csharp
var users = new Table<User>(session);
```

New `Table<T>` (`IQueryable`) instances can be created each time they are needed, as short-lived instances, as long as you are reusing the same `ISession` and `MappingConfiguration` instances. `MappingConfiguration.Global` is used if you don't provide one.

In some scenarios, it might make sense to create multiple `MappingConfiguration` (e.g. reuse the same POCOs for multiple keyspaces).

## Example

```csharp
public class User
{
   public Guid UserId { get; set; }
   public string Name { get; set; }
   public string Group { get; set; }
}

// Get a list of users using a Linq query
IEnumerable<User> adminUsers =
      (from user in users where user.Group == "admin" select user).Execute();
```

You can also write your queries using lambda syntax

```csharp
IEnumerable<User> adminUsers = users
      .Where(u => u.Group == "admin")
      .Execute();
```

The Linq component creates new instances of your classes using its parameter-less constructor.

## Configuring mappings

In many scenarios, you need more control over how your class maps to a CQL table. You have two ways of configuring with Linq:

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

You should map one C# class per table. The Linq component of the driver will use the configuration defined when creating the `Table<T>` instance to determine to which keyspace and table it maps to, using `MappingConfiguration.Global` when not specified.

## Linq API examples

The simple query example is great, but the Linq component has a lot of other methods for doing Inserts, Updates, Deletes, and even Create table. Including Linq operations `Where()`, `Select()`, `OrderBy()`, `OrderByDescending()`, `First()`, `Count()`, and `Take()`, it translates into the most efficient CQL query possible, trying to retrieve as less data possible.

For example, the following query only retrieves the username from the cluster to fill in a lazy list of string on the client side.

```csharp
IEnumerable<string> userNames = ( from user in users where user.Group == "admin" select user.Name).Execute();
```

Some other examples:

```csharp
// First row or null using a query
User user = (
   from user in users where user.Group == "admin"
   select user.Name).FirstOrDefault().Execute();

// First row or null using lambda syntax
User user = users.Where(u => u.UserId == "john")
      .FirstOrDefault()
      .Execute();

// Use Take() to limit your result sets server side
var userAdmins = (
   from user in users where user.Group == "admin"
   select user.Name).Take(100).Execute();

// Use Select() to project to a new form server side
var userCoordinates = (
   from user in users where user.Group == "admin"
   select new Tuple(user.X, user.Y)).Execute();

// Delete
users.Where(u => u.UserId == "john")
      .Delete()
      .Execute();

// Delete If (Cassandra 2.1+)
users.Where(u => u.UserId == "john")
      .DeleteIf(u => u.LastAccess == value)
      .Execute();

// Update
users.Where(u => u.UserId == "john")
      .Select(u => new User { LastAccess = TimeUuid.NewId()})
      .Update()
      .Execute();
```
