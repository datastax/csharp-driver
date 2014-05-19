# Datastax C# Driver for Apache Cassandra

A C# client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's binary protocol.

## Installation

[Get it on Nuget](https://nuget.org/packages/CassandraCSharpDriver/)
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


## Basic Usage

```csharp
//Create a cluster instance using 2 cassandra nodes.
var cluster = Cluster.Builder()
  .AddContactPoint("127.0.0.1")
  .AddContactPoint("127.0.0.2")
  .Build();

//Create connections to the nodes using a keyspace
var session = cluster.Connect("sample_keyspace");

//Execute a query on a connection synchronously
var rs = session.Execute("SELECT * FROM sample_table");

foreach (var row in rs)
{
  var value = row.GetValue<int>("sample_int_column");
  //do something with the value
}
```

### Async sample

```csharp
//Execute a query on a connection asynchronously using TPL
var task = session.ExecuteAsync(query);
task.ContinueWith((t) =>
{
  var rs = t.Result;
  foreach (var row in rs)
  {
    var value = row.GetValue<int>("sample_int_column");
    //do something with the value
  }
});
```

## Documentation

- [Documentation index for v1](http://www.datastax.com/documentation/developer/csharp-driver/1.0/webhelp/index.html).
- [API docs for v1](http://www.datastax.com/drivers/csharp/apidocs/).

## Getting Help

You can use the project [Mailing list](https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user) or create a ticket on the [Jira issue tracker](https://datastax-oss.atlassian.net/browse/CSHARP).

## Upgrading from 1.x branch

If you are upgrading from the 1.x branch of the driver, be sure to have a look at [the upgrade guide](https://github.com/datastax/csharp-driver/blob/2.0/doc/upgrade-guide-2.0.md).

## Building and running the tests

You can use Visual Studio or msbuild to build the solution. 

[Check the documentation for driver developers and testers](https://github.com/datastax/csharp-driver/wiki/Building-and-running-tests).

## License
Copyright 2013, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
