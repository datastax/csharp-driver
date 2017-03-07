# Result paging

## Automatic paging 

You can iterate indefinitely over the `RowSet`, having the rows fetched block by block until the rows available on the
client side are exhausted.

```csharp
var ps = session.Prepare("SELECT * from tbl1 WHERE key = ?");
// Set the page size at statement level.
var statement = ps.Bind(key).SetPageSize(1000);
var rs = session.Execute(statement);
foreach (var row in rs)
{
   // The enumerator will yield all the rows from Cassandra.
   // Retrieving them in the back in blocks of 1000.
}
```

## Manual paging 

If you want to retrieve the next page of results only when you ask for it (for example, in a webpager), use the
`PagingState` property in the `RowSet` to execute the following statement.

```csharp
var ps = session.Prepare("SELECT * from tbl1 WHERE key = ?");
// Disable automatic paging.
var statement = ps
   .Bind(key)
   .SetAutoPage(false)
   .SetPageSize(pageSize);
var rs = session.Execute(statement);
// Store the paging state
var pagingState = rs.PagingState;

// Later in time ...
// Retrieve the following page of results.
var statement2 = ps
   .Bind(key)
   .SetAutoPage(false)
   .SetPagingState(pagingState)
var rs2 = Session.Execute(statement2);
```

Note: The `PagingState` property is not encrypted and can be used to inject values to retrieve other partitions, so be
careful not to expose it to the end user.

## Automatic paging in LINQ and Mapper components 

Both LINQ and Mapper queries support automatic paging: as you iterate through the mapped results, it fetches the
following pages. If you want to manually page, you can use Linq's `ExecutePaged()` method, Mapper's `FetchPage()`, or
their async counterparts.

A LINQ paging example:

```csharp
// Providing page size.
IPage<User> adminUsers = users
   .Where(u => u.Group == "admin")
   .SetPageSize(pageSize)
   .ExecutePaged();


// Providing paging state (following pages).
IPage<User> adminUsers = users
   .Where(u => u.Group == "admin")
   .SetPageSize(pageSize)
   .SetPagingState(pagingState)
   .ExecutePaged();
```

A Mapper paging example:

```csharp
IPage<User> users = mapper.FetchPage<User>(pageSize, pagingState, query, parameters);

// Or using query options

IPage<User> authors = mapper.FetchPage<User>(
      Cql.New(query, parameters).WithOptions(opt =>
            opt.SetPageSize(pageSize).SetPagingState(state)));
```