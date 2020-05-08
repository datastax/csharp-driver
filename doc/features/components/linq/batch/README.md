# Batch statements with LINQ

This section shows how to use the LINQ API for batch statements. If you're interested in reading more about how Batch Statements work in Cassandra, check [the Batch Statements section][batchstatements].

## Normal Batches

Let's take the example shown in the [LINQ overview documentation](../) and add another class:

```csharp
public class User
{
   public Guid UserId { get; set; }
   public string Name { get; set; }
}

public class UserByName : User
{
}
```

Let's define the mappings:

```csharp
public class MyMappings : Mappings
{
   public MyMappings()
   {
      For<User>()
         .KeyspaceName("ks_example")
         .TableName("users")
         .PartitionKey(u => u.UserId)
         .Column(u => u.UserId, cm => cm.WithName("id"))
         .Column(u => u.Name, cm => cm.WithName("name"));

      For<UserByName>()
         .KeyspaceName("ks_example")
         .TableName("users_by_name")
         .PartitionKey(u => u.UserId)
         .Column(u => u.UserId, cm => cm.WithName("id"))
         .Column(u => u.Name, cm => cm.WithName("name"));
   }
}
```

Don't forget to actually define the `MappingConfiguration` and create the `Table` instances:

```csharp
MappingConfiguration.Global.Define<MyMappings>();
var usersTable = new Table<User>(session);
var usersByNameTable = new Table<UserByName>(session);
```

Here is an example of creating a batch that inserts a user to both tables:

```csharp
var user = new User
{
      Name = "john",
      UserId = Guid.NewGuid()
};

var userByName = new UserByName
{
      Name = user.Name,
      UserId = user.UserId
};

var batch = usersTable.GetSession().CreateBatch(BatchType.Logged);

batch.Append(usersTable.Insert(user));
batch.Append(usersByNameTable.Insert(userByName));

await batch.ExecuteAsync().ConfigureAwait(false);
```

## Conditional Batches

The LINQ API doesn't have support for conditional batches. It is still possible to execute conditional batches using the methods that are used in the previous example for normal batches but there is no way to obtain the response that the server returns for conditional batches.

You can use the Mapper without having to change your mapping configuration code (it works with both attribute based configuration and fluent based configuration just like LINQ) so you could use the Mapper for this use case. Check out the `Conditional Batches` section on [the Mapper documentation](../../mapper/batch).

[batchstatements]: ../../core/statements/batch