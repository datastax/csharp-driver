# Datastax C# Driver for Apache Cassandra

A C# client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's binary protocol.

## Installation

[Get it on Nuget][nuget]
```bash
PM> Install-Package CassandraCSharpDriver
```

## Features

- Connection pooling
- Node discovery
- Automatic failover
- Several load balancing and retry policies
- Result paging
- Query batching
- Linq2Cql and Ado.Net support

## Documentation

- [API docs][apidocs]
- [Documentation index][docindex]

## Getting Help

You can use the project [Mailing list][mailinglist] or create a ticket on the [Jira issue tracker][jira].

## Upgrading from 1.x branch

If you are upgrading from the 1.x branch of the driver, be sure to have a look at the [upgrade guide](https://github.com/datastax/csharp-driver/blob/2.0/doc/upgrade-guide-2.0.md).

## Basic Usage

```csharp
//Create a cluster instance using 3 cassandra nodes.
var cluster = Cluster.Builder()
  .AddContactPoints("host1", "host2", "host3")
  .Build();
//Create connections to the nodes using a keyspace
var session = cluster.Connect("sample_keyspace");
//Execute a query on a connection synchronously
var rs = session.Execute("SELECT * FROM sample_table");
//Iterate through the RowSet
foreach (var row in rs)
{
  var value = row.GetValue<int>("sample_int_column");
  //do something with the value
}
```

### Prepared statements

Prepare your query **once** and bind different parameters to obtain the better performance.

```csharp
//Prepare a statement once
var ps = session.Prepare("UPDATE user_profiles SET birth=? WHERE key=?");

//...bind different parameters every time you need to execute
var statement = ps.Bind(new DateTime(1942, 11, 27), "hendrix");
//Execute the bound statement with the provided parameters
session.Execute(statement);
```

### Batching statements

You can execute multiple statements (prepared or unprepared) in a batch to update/insert several rows atomically even in different column families.

```csharp
//Prepare the statements involved in a profile update once
var profileStmt = session.Prepare("UPDATE user_profiles SET email=? WHERE key=?");
var userTrackStmt = session.Prepare("INSERT INTO user_track (key, text, date) VALUES (?, ?, ?)");
//...you should reuse the prepared statement
//Bind the parameters and add the statement to the batch batch
var batch = new BatchStatement()
  .Add(profileStmt.Bind(emailAddress, "hendrix"))
  .Add(userTrackStmt.Bind("hendrix", "You changed your email", DateTime.Now));
//Execute the batch
session.Execute(batch);
```

### Asynchronous API

Session allows asynchronous execution of statements (for any type of statement: simple, bound or batch) by exposing the `ExecuteAsync` method.

```csharp
//Execute a statement asynchronously using await
var rs = await session.ExecuteAsync(statement);
```

Or if you want to continue or wait for the async task to complete.

```csharp
//Execute a statement asynchronously using TPL
var task = session.ExecuteAsync(statement);
//The task can waited, awaited, continued, ...
task.ContinueWith((t) =>
{
  var rs = t.Result;
  //Iterate through the rows
  foreach (var row in rs)
  {
    //Get the values from each row
  }
}, TaskContinuationOptions.OnlyOnRanToCompletion);
```

### Automatic pagination of results

You can iterate indefinitely over the `RowSet`, having the rows fetched block by block until the rows available on the client side are exhausted.

```csharp
var statement = new SimpleStatement("SELECT * from large_table");
//Set the page size, in this case the RowSet will not contain more than 1000 at any time
statement.SetPageSize(1000);
var rs = session.Execute(statement);
foreach (var row in rs)
{
  //The enumerator will yield all the rows from Cassandra
  //Retrieving them in the back in blocks of 1000.
}
```

### Setting cluster and statement execution options

You can set the options on how the driver connects to the nodes and the execution options.

```csharp
//Example at cluster level
var cluster = Cluster
  .Builder()
  .AddContactPoints(hosts)
  .WithCompression(CompressionType.LZ4)
  .WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("west"));

//Example at statement (simple, bound, batch) level
var statement = new SimpleStatement(query)
  .SetConsistencyLevel(ConsistencyLevel.Quorum)
  .SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
  .SetPageSize(1000);
```

## Building and running the tests

You can use Visual Studio or msbuild to build the solution. 

[Check the documentation for building the driver from source and running the tests](https://github.com/datastax/csharp-driver/wiki/Building-and-running-tests).

## License
Copyright 2014, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

  [apidocs]: http://www.datastax.com/drivers/csharp/2.0/
  [docindex]: http://www.datastax.com/documentation/developer/csharp-driver/2.0/
  [nuget]: https://nuget.org/packages/CassandraCSharpDriver/
  [mailinglist]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user
  [jira]: https://datastax-oss.atlassian.net/browse/CSHARP