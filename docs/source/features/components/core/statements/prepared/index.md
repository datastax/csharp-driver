# Prepared statements

Use prepared statements for queries that are executed multiple times in your application:

```csharp
PreparedStatement prepared = session.Prepare("insert into product (sku, description) values (?, ?)");
BoundStatement bound = prepared.Bind("234827", "Mouse");
session.Execute(bound);
```

When you prepare the statement, Cassandra parses the query string, caches the result and returns a unique identifier (the `PreparedStatement` object keeps an internal reference to that identifier):

```ditaa
client                   driver           Cassandra
--+------------------------+----------------+------
  |                        |                |
  | session.prepare(query) |                |
  |----------------------->|                |
  |                        | PREPARE(query) |
  |                        |--------------->|
  |                        |                |
  |                        |                |
  |                        |                | - compute id
  |                        |                | - parse query string
  |                        |                | - cache (id, parsed)
  |                        |                |
  |                        | PREPARED(id)   |
  |                        |<---------------|
  |  PreparedStatement(id) |                |
  |<-----------------------|                |
```

When you bind and execute a prepared statement, the driver only sends the identifier, which allows Cassandra to skip the parsing phase:

```ditaa
client                            driver                Cassandra
--+---------------------------------+---------------------+------
  |                                 |                     |
  | session.execute(BoundStatement) |                     |
  |-------------------------------->|                     |
  |                                 | EXECUTE(id, values) |
  |                                 |-------------------->|
  |                                 |                     |
  |                                 |                     |
  |                                 |                     | - get cache(id)
  |                                 |                     | - execute query
  |                                 |                     |
  |                                 |          ROWS       |
  |                                 |<--------------------|
  |                                 |                     |
  |<--------------------------------|                     |
```

## Advantages of prepared statements

Beyond saving a bit of parsing overhead on the server, prepared statements have other advantages; the `PREPARED` response also contains useful metadata about the CQL query:

* information about the result set that will be produced when the statement gets executed. The driver caches this, so that the server doesn't need to include it with every response. This saves a bit of bandwidth, and the resources it would take to decode it every time. This is only enabled for protocol v5+, i.e., Apache Cassandra 4.0+. ScyllaDB uses protocol v4 with custom extensions, and the extension that will enable the said optimisation is in progress.
* the CQL types of the bound variables. This allows the `PreparedStatement.Bind` method to perform better checks, and fail fast (without a server round-trip) if the types are wrong.
* which bound variables are part of the partition key. This allows bound statements to automatically compute their [routing key](../../../../routing-queries/index).
* more optimizations might get added in the future. For example, [CASSANDRA-10813] suggests adding an `idempotent` flag to the response.

If you have a unique query that is executed only once, a [simple statement](../simple/index) will be more efficient. But note that this should be pretty rare: most client applications typically repeat the same queries over and over, and a parameterized version can be extracted and prepared.

## Preparing

`Session.Prepare()` accepts a plain query string.

We recommend avoiding repeated calls to `Prepare()` because the driver does not cache prepared statements so there could be performance issues if the same query is prepared multiple times.

## Parameters and binding

The prepared query string will usually contain placeholders, which can be either anonymous or named:

```csharp
ps1 = session.Prepare("insert into product (sku, description) values (?, ?)");
ps2 = session.Prepare("insert into product (sku, description) values (:s, :d)");
```

To turn the statement into its executable form, you need to *bind* it in order to create a `BoundStatement`. As shown previously, there is a shorthand to provide the parameters in the same call:

```csharp
BoundStatement bound = ps1.Bind("324378", "LCD screen");
```

### Unset values

With [native protocol](../../../../native-protocol/index) V3, all variables must be bound. With native protocol V4, variables can be left unset, in which case they will be ignored (no tombstones will be generated). If you're reusing a bound statement, you can use the `unset` method to unset variables that were previously set:

```csharp
BoundStatement bound = ps1.bind("324378", Unset.Value);
```

## How the driver prepares

Cassandra does not replicate prepared statements across the cluster. It is the driver's responsibility to ensure that each node's cache is up to date. It uses a number of strategies to achieve this:

1.  When a statement is initially prepared, it is first sent to a single node in the cluster (this avoids hitting all nodes in case the query string is wrong). Once that node replies successfully, the driver re-prepares on all remaining nodes:

    ```ditaa
    client                   driver           node1          node2  node3
    --+------------------------+----------------+--------------+------+---
      |                        |                |              |      |
      | session.prepare(query) |                |              |      |
      |----------------------->|                |              |      |
      |                        | PREPARE(query) |              |      |
      |                        |--------------->|              |      |
      |                        |                |              |      |
      |                        | PREPARED(id)   |              |      |
      |                        |<---------------|              |      |
      |                        |                |              |      |
      |                        |                |              |      |
      |                        |           PREPARE(query)      |      |
      |                        |------------------------------>|      |
      |                        |                |              |      |
      |                        |           PREPARE(query)      |      |
      |                        |------------------------------------->|
      |                        |                |              |      |
      |<-----------------------|                |              |      |
    ```

    The prepared statement identifier is deterministic (it's a hash of the query string), so it is the same for all nodes.

2.  if a node crashes, it might lose all of its prepared statements (this depends on the version: since Cassandra 3.10, prepared statements are stored in a table, and the node is able to reprepare on its own when it restarts). So the driver keeps a client-side cache; anytime a node is marked back up, the driver re-prepares all statements on it;

3.  finally, if the driver tries to execute a statement and finds out that the coordinator doesn't know about it, it will re-prepare the statement on the fly (this is transparent for the client, but will cost two extra roundtrips):

    ```ditaa
    client                          driver                         node1
    --+-------------------------------+------------------------------+--
      |                               |                              |
      |session.execute(boundStatement)|                              |
      +------------------------------>|                              |
      |                               |     EXECUTE(id, values)      |
      |                               |----------------------------->|
      |                               |                              |
      |                               |         UNPREPARED           |
      |                               |<-----------------------------|
      |                               |                              |
      |                               |                              |
      |                               |       PREPARE(query)         |
      |                               |----------------------------->|
      |                               |                              |
      |                               |        PREPARED(id)          |
      |                               |<-----------------------------|
      |                               |                              |
      |                               |                              |
      |                               |     EXECUTE(id, values)      |
      |                               |----------------------------->|
      |                               |                              |
      |                               |             ROWS             |
      |                               |<-----------------------------|
      |                               |                              |
      |<------------------------------|                              |
    ```

You can customize these strategies through the `Builder.QueryOptions()` method:

* `QueryOptions.SetPrepareOnAllHosts()` controls whether statements are initially re-prepared on other hosts (step 1 above);
* `QueryOptions.SetReprepareOnUp` controls whether statements are re-prepared on a node that comes back up (step 2 above).

[CASSANDRA-10813]: https://issues.apache.org/jira/browse/CASSANDRA-10813
