# ScyllaDB C# Driver for Scylla

ScyllaDB's fork of a modern, [feature-rich][features] and highly tunable C# client library for Scylla using Cassandra's binary protocol and Cassandra Query Language v3.

The driver targets .NET Framework 4.5.2 and .NET Standard 2.0. For more detailed information about platform compatibility, check [this section](#compatibility).

## Installation

[Get it on Nuget][nuget]

```bash
PM> Install-Package ScyllaDBCSharpDriver
```

[![Latest stable](https://img.shields.io/nuget/v/ScyllaDBCSharpDriver.svg)](https://www.nuget.org/packages/ScyllaDBCSharpDriver)

## Features

- Sync and [Async](#asynchronous-api) API
- Simple, [Prepared](#prepared-statements), and [Batch](#batching-statements) statements
- Asynchronous IO, parallel execution, request pipelining
- Connection pooling
- Auto node discovery
- Automatic reconnection
- Configurable [load balancing][policies] and [retry policies][policies]
- Works with any cluster size
- [Linq2Cql][linq] and Ado.Net support

## ScyllaDB features

- Shard awarness
- Tablet awareness
- LWT prepared statements metadata mark

## Documentation

- [Documentation index][docindex]
- [API docs][apidocs]
- [FAQ][faq]
- [Developing applications with ScyllaDB drivers][dev-guide]

## Getting Help

You can create a ticket on the [Github Issues][github-issues]. Additionally, you can ask questions on [ScyllaDB Community][community].

## Upgrading from previous versions

If you are upgrading from previous versions of the driver, [visit the Upgrade Guide][upgrade-guide].

## Basic Usage

```csharp
// Configure the builder with your cluster's contact points
var cluster = Cluster.Builder()
                     .AddContactPoints("host1")
                     .Build();

// Connect to the nodes using a keyspace
var session = cluster.Connect("sample_keyspace");

// Execute a query on a connection synchronously
var rs = session.Execute("SELECT * FROM sample_table");

// Iterate through the RowSet
foreach (var row in rs)
{
    var value = row.GetValue<int>("sample_int_column");

    // Do something with the value
}
```

### Prepared statements

Prepare your query **once** and bind different parameters to obtain best performance.

```csharp
// Prepare a statement once
var ps = session.Prepare("UPDATE user_profiles SET birth=? WHERE key=?");

// ...bind different parameters every time you need to execute
var statement = ps.Bind(new DateTime(1942, 11, 27), "hendrix");
// Execute the bound statement with the provided parameters
session.Execute(statement);
```

### Batching statements

You can execute multiple statements (prepared or unprepared) in a batch to update/insert several rows atomically even in different column families.

```csharp
// Prepare the statements involved in a profile update once
var profileStmt = session.Prepare("UPDATE user_profiles SET email=? WHERE key=?");
var userTrackStmt = session.Prepare("INSERT INTO user_track (key, text, date) VALUES (?, ?, ?)");
// ...you should reuse the prepared statement
// Bind the parameters and add the statement to the batch batch
var batch = new BatchStatement()
  .Add(profileStmt.Bind(emailAddress, "hendrix"))
  .Add(userTrackStmt.Bind("hendrix", "You changed your email", DateTime.Now));
// Execute the batch
session.Execute(batch);
```

### Asynchronous API

Session allows asynchronous execution of statements (for any type of statement: simple, bound or batch) by exposing the `ExecuteAsync` method.

```csharp
// Execute a statement asynchronously using await
var rs = await session.ExecuteAsync(statement);
```

### Avoid boilerplate mapping code

The driver features a built-in [Mapper][mapper] and [Linq][linq] components that can use to avoid boilerplate mapping code between cql rows and your application entities.

```csharp
User user = mapper.Single<User>("SELECT name, email FROM users WHERE id = ?", userId);
```

See the [driver components documentation][components] for more information.

### Automatic pagination of results

You can iterate indefinitely over the `RowSet`, having the rows fetched block by block until the rows available on the client side are exhausted.

```csharp
var statement = new SimpleStatement("SELECT * from large_table");
// Set the page size, in this case the RowSet will not contain more than 1000 at any time
statement.SetPageSize(1000);
var rs = session.Execute(statement);
foreach (var row in rs)
{
  // The enumerator will yield all the rows from Cassandra
  // Retrieving them in the back in blocks of 1000.
}
```

### User defined types mapping

You can map your [Cassandra User Defined Types][udt] to your application entities.

For a given udt

```cql
CREATE TYPE address (
  street text,
  city text,
  zip_code int,
  phones set<text>
);
```

For a given class

```csharp
public class Address
{
  public string Street { get; set; }
  public string City { get; set; }
  public int ZipCode { get; set; }
  public IEnumerable<string> Phones { get; set;}
}
```

You can either map the properties by name

```csharp
// Map the properties by name automatically
session.UserDefinedTypes.Define(
  UdtMap.For<Address>()
);
```

Or you can define the properties manually

```csharp
session.UserDefinedTypes.Define(
  UdtMap.For<Address>()
    .Map(a => a.Street, "street")
    .Map(a => a.City, "city")
    .Map(a => a.ZipCode, "zip_code")
    .Map(a => a.Phones, "phones")
);
```

You should **map your [UDT][udt] to your entity once** and you will be able to use that mapping during all your application lifetime.

```csharp
var rs = session.Execute("SELECT id, name, address FROM users where id = x");
var row = rs.First();
// You can retrieve the field as a value of type Address
var userAddress = row.GetValue<Address>("address");
Console.WriteLine("user lives on {0} Street", userAddress.Street);
```

### Setting cluster and statement execution options

You can set the options on how the driver connects to the nodes and the execution options.

```csharp
// Example at cluster level
var cluster = Cluster
  .Builder()
  .AddContactPoints(hosts)
  .WithCompression(CompressionType.LZ4)
  .WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("west"));

// Example at statement (simple, bound, batch) level
var statement = new SimpleStatement(query)
  .SetConsistencyLevel(ConsistencyLevel.Quorum)
  .SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
  .SetPageSize(1000);
```

## Authentication

If you are using the `PasswordAuthenticator` which is included in the default distribution of Apache Cassandra, you can use the `Builder.WithCredentials` method or you can explicitly create a `PlainTextAuthProvider` instance.

To configure a provider, pass it when initializing the cluster:

```csharp
using Cassandra;
```

```csharp
ICluster cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithAuthProvider(new PlainTextAuthProvider())
    .Build();
```

## Compatibility

- ScyllaDB 2025.1 and above.
- ScyllaDB 5.x and above.
- ScyllaDB Enterprise 2021.x and above.
- The driver targets .NET Framework 4.5.2 and .NET Standard 2.0

Here is a list of platforms and .NET targets that Datastax uses when testing this driver:

|  Platform             | net462 | net472 | net481 | net6 | net7 | net8  |
|-----------------------|--------|--------|--------|------|------|-------|
| Windows Server 2019³  |  ✓    |  ✓     |  ✓     |  ✓²  |  ✓¹ |  ✓   |
| Ubuntu 18.04          |  -     |  -     |   -    |  ✓   | ✓   | ✓    |

¹ No tests are run for the `net7` target on the Windows platform but `net7` is still considered fully supported.

² Only unit tests are ran for the `net6` target on the windows platform but `net6` is still considered fully supported.

³ Appveyor's `Visual Studio 2022` image is used for these tests.

Mono `6.12.0` is also used to run `net462` tests on `Ubuntu 18.04` but Datastax can't guarantee that the driver fully supports Mono in a production environment. Datastax recommends the modern cross platform .NET platform instead.

Note: Big-endian systems are not supported.

## Building and running the tests

You can use Visual Studio or msbuild to build the solution.

[Check the documentation for building the driver from source and running the tests](https://github.com/datastax/csharp-driver/wiki/Building-and-running-tests).

## License

© DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[apidocs]: https://docs.datastax.com/en/latest-csharp-driver-api/
[docindex]: https://csharp-driver.docs.scylladb.com/stable/
[features]: https://csharp-driver.docs.scylladb.com/stable/features/index.html
[faq]: https://csharp-driver.docs.scylladb.com/stable/faq/index.html
[nuget]: https://nuget.org/packages/ScyllaDBCSharpDriver/
[github-issues]: https://github.com/scylladb/csharp-driver/issues
[udt]: https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useCreateUDT.html
[poco]: http://en.wikipedia.org/wiki/Plain_Old_CLR_Object
[linq]: https://csharp-driver.docs.scylladb.com/stable/features/components/linq/index.html
[mapper]: https://csharp-driver.docs.scylladb.com/stable/features/components/mapper/index.html
[components]: https://csharp-driver.docs.scylladb.com/stable/features/components/index.html
[policies]: https://csharp-driver.docs.scylladb.com/stable/features/tuning-policies/index.html
[upgrade-guide]: https://csharp-driver.docs.scylladb.com/stable/upgrade-guide/index.html
[community]: https://forum.scylladb.com/
[implicit]: https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/implicit
[dynamic]: https://msdn.microsoft.com/en-us/library/dd264736.aspx
[dev-guide]: https://docs.scylladb.com/stable/get-started/develop-with-scylladb/index.html

:::{toctree}
:hidden:
:maxdepth: 1


features/index
faq/index
upgrade-guide/index
examples/index
datastax-api-reference.md
:::
