# Cluster and schema metadata

You can retrieve the cluster topology and the schema metadata information using the C# driver.

After establishing the first connection, the driver retrieves the cluster topology details and exposes these through methods on the `Metadata` class. The `Metadata` instance for a given cluster can be accessed through the `Cluster.Metadata` property.

## Metadata Synchronization

The information mentioned before is kept up to date using Cassandra event notifications.

It's this Metadata synchronization process that computes the internal `TokenMap` which is necessary for [token aware query routing](../routing-queries) to work correctly.

By default, this feature is enabled but it can be disabled:

```csharp
var cluster = 
   Cluster.Builder()
          .AddContactPoint("127.0.0.1")
          .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
          .Build();
```

## Retrieving metadata

The following example outputs hosts information about your cluster:

```csharp
foreach (var host in cluster.AllHosts())
{
   Console.WriteLine($"{host.Address}, {host.Datacenter}, {host.Rack}");
}
```

Additionally, the keyspaces information is already loaded into the `Metadata` object, once the client is connected (when matadata synchronization is enabled):

```csharp
foreach (var keyspace in cluster.Metadata.GetKeyspaces())
{
   Console.WriteLine(keyspace);
}
```

To retrieve the definition of a table, use the `Metadata.GetTable()` method:

```csharp
var tableInfo = await cluster.Metadata.GetTableAsync("keyspace", "table").ConfigureAwait(false);
Console.WriteLine($"Table {tableInfo.Name}");
foreach (var c in tableInfo.ColumnsByName)
{
   Console.WriteLine($"Column {c.Value.Name} with type {c.Value.TypeCode}");
}
```

When metadata synchronization is enabled, table metadata is cached on the first request for that specific table and the cache gets evicted whenever schema or topology changes happen that affect the table's keyspace.

## Schema agreement

Schema changes need to be propagated to all nodes in the cluster. Once they have settled on a common version, we say that they are in agreement.

The driver waits for schema agreement after executing a schema-altering query. This is to ensure that subsequent requests (which might get routed to different nodes) see an up-to-date version of the schema.

```ditaa
 Application             Driver           Server
------+--------------------+------------------+-----
      |                    |                  |
      |  CREATE TABLE...   |                  |
      |------------------->|                  |
      |                    |   send request   |
      |                    |----------------->|
      |                    |                  |
      |                    |     success      |
      |                    |<-----------------|
      |                    |                  |
      |          /--------------------\       |
      |          :Wait until all nodes+------>|
      |          :agree (or timeout)  :       |
      |          \--------------------/       |
      |                    |        ^         |
      |                    |        |         |
      |                    |        +---------|
      |                    |                  |
      |                    |  refresh schema  |
      |                    |----------------->|
      |                    |<-----------------|
      |   complete query   |                  |
      |<-------------------|                  |
      |                    |                  |
```

The schema agreement wait is performed when a `SCHEMA_CHANGED` response is received when executing a request. The task returned by `Session.ExecuteAsync` and other similar methods in Mapper and LINQ components will only be complete after the schema agreement wait (or until the timeout specified with `Builder.WithMaxSchemaAgreementWaitSeconds`). The same applies to synchronous methods like `Session.Execute`, i.e. they will only return after the schema agreement wait. Note that when the schema agreement wait returns due to a timeout, no exception will be thrown but nodes won't be in agreement.

The check is implemented by repeatedly querying system tables for the schema version reported by each node, until they all converge to the same value. If that doesn't happen within a given timeout, the driver will give up waiting.
The default timeout is `10` seconds, it can be customized when creating the `Cluster` instance:

```csharp
var cluster = 
   Cluster.Builder()
          .AddContactPoint("127.0.0.1")
          .WithMaxSchemaAgreementWaitSeconds(5)
          .Build();
```

After executing a statement, you can check whether schema agreement was successful or timed out:

```csharp
var rowSet = await session.ExecuteAsync("CREATE TABLE table1 (id int PRIMARY KEY)").ConfigureAwait(false);
Console.WriteLine($"Is schema in agreement? {rowSet.Info.IsSchemaInAgreement}");
```

Additionally, you can perform an on-demand check at any time:

```csharp
var isSchemaInAgreement = await session.Cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false);
```

Note that the on-demand check using `Metadata.CheckSchemaAgreementAsync()` does not retry, it only queries system tables once.