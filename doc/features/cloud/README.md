# Connecting to DataStax Astra

## Quickstart

Use the `Builder.WithCloudSecureConnectionBundle(string path)` method to connect to your [DataStax Astra database] using your secure connection bundle (`secure-connect-DATABASE_NAME.zip`).

Also, use `Builder.WithCredentials(string username, string password)` to provide your [CQL credentials].

Here is an example of the minimum configuration needed to connect to your DataStax Astra database using the secure connection bundle:

```csharp
var session = 
    Cluster.Builder()
           .WithCloudSecureConnectionBundle(@"C:\path\to\secure-connect-DATABASE_NAME.zip")
           .WithCredentials("user_name", "password")
           .Build()
           .Connect();
```

## Configurable settings when using a secure connection bundle

The following methods will throw an error when `.Build()` is called if the secure connection bundle is used:

- `Builder.WithSSL(...)`
- `Builder.AddContactPoint(...)` and `Builder.AddContactPoints(...)`

Every other method in the `Builder` class will have the same effect whether you are using a secure connection bundle or not.

## Minimal example project

You can find a minimal .NET Core Console Application project in the examples folder of the driver's [Github Repository].

[DataStax Astra database]: https://www.datastax.com/cloud/datastax-astra
[CQL credentials]: http://cassandra.apache.org/doc/latest/cql/security.html#cql-roles
[Github Repository]: https://github.com/datastax/csharp-driver/tree/master/examples/SecureConnectionBundle/MinimalExample
