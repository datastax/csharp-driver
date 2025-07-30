# Batch statements with the Mapper

This section shows how to use the Mapper API for batch statements. If you're interested in reading more about how Batch Statements work in Cassandra, check [the Batch Statements section][batchstatements].

## Normal Batches

Let's take the example shown in the [Mapper overview documentation](../index) and add another class:

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

Don't forget to actually define the `MappingConfiguration` and create the `IMapper` instance:

```csharp
MappingConfiguration.Global.Define<MyMappings>();
var mapper = new Mapper(session);
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

var batch = mapper.CreateBatch(BatchType.Logged);

batch.Insert(user);
batch.Insert(userByName);

await mapper.ExecuteAsync(batch).ConfigureAwait(false);
```

## Conditional Batches

Let's take the schema example from [this DataStax documentation page][batch-static] and simplify it a bit:

```csharp
private class Purchase
{
   public Guid UserId { get; set; }

   public Guid ExpenseId { get; set; }

   public int Amount { get; set; }

   public int Balance { get; set; }
}

private class MyMappings : Mappings
{
   public MyMappings()
   {
         For<Purchase>()
            .KeyspaceName("ks_example")
            .TableName("purchases")
            .PartitionKey(u => u.UserId)
            .ClusteringKey(u => u.ExpenseId)
            .Column(u => u.UserId, cm => cm.WithName("user_id"))
            .Column(u => u.ExpenseId, cm => cm.WithName("expense_id"))
            .Column(u => u.Amount, cm => cm.WithName("amount"))
            .Column(u => u.Balance, cm => cm.WithName("balance").AsStatic());
   }
}
```

Here's how you can execute a conditional batch and parse the response:

```csharp
var batchCond = mapper.CreateBatch(BatchType.Logged);

batchCond.Execute("INSERT INTO ks_example.purchases (user_id, balance) " +
   "VALUES (273201dc-0a30-4f8e-a9d2-e383df1c6a8b, -8) IF NOT EXISTS");
batchCond.Execute(
   "INSERT INTO ks_example.purchases (user_id, expense_id, amount) " +
      "VALUES (273201dc-0a30-4f8e-a9d2-e383df1c6a8b, 5717f71e-0cc8-4a97-a960-0b9f8ca5a487, 8)");

var appliedInfo = await mapper.ExecuteConditionalAsync<Purchase>(batchCond).ConfigureAwait(false);

if (!appliedInfo.Applied)
{
      Console.WriteLine(
         "There's already an entry for that user. User's balance: " + appliedInfo.Existing.Balance);
}
```

 The server response contains a boolean that specifies whether the batch was applied and can contain the existing row, see the `AppliedInfo<T>` object that is returned by `Mapper.ExecuteConditional()`.

[batchstatements]: ../../core/statements/batch/index
[batch-static]: https://docs.datastax.com/en/cql-oss/3.1/cql/cql_using/use-batch-static.html