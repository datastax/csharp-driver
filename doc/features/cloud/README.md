# Connecting to DataStax Astra

## Quickstart

Use the `DseClusterBuilder.WithCloudSecureConnectionBundle(string path)` method to connect to your [DataStax Astra database] using your secure connection bundle (`secure-connect-DATABASE_NAME.zip`).

Also, use `DseClusterBuilder.WithCredentials(string username, string password)` to provide your [CQL credentials].

Here is an example of the minimum configuration needed to connect to your DataStax Astra database using the secure connection bundle:

```csharp
var session = 
    DseCluster.Builder()
           .WithCloudSecureConnectionBundle(@"C:\path\to\secure-connect-DATABASE_NAME.zip")
           .WithCredentials("user_name", "password")
           .Build()
           .Connect();
```

## Supported platforms

DataStax Astra support on .NET Core requires **.NET Core Runtime 2.1 or later**.

For the remaining platforms supported by the driver, there is no additional requirement.

## Configurable settings when using a secure connection bundle

The following methods will throw an error when `.Build()` is called if the secure connection bundle is used:

- `DseClusterBuilder.WithSSL(...)`
- `DseClusterBuilder.AddContactPoint(...)` and `DseClusterBuilder.AddContactPoints(...)`

Every other method in the `DseClusterBuilder` class will have the same effect whether you are using a secure connection bundle or not.

[DataStax Astra database]: https://www.datastax.com/cloud/datastax-astra
[CQL credentials]: http://cassandra.apache.org/doc/latest/cql/security.html#cql-roles
