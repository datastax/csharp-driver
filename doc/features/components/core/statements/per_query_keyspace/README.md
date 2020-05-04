# Per-query keyspace

Sometimes it is convenient to send the keyspace separately from the query string, and without switching the whole session to that keyspace either. For example, you might have a multi-tenant setup where identical requests are executed against different keyspaces.

**This feature is only available with Cassandra 4.0 or above** ([CASSANDRA-10145]). Make sure you are using [native protocol](../../../../native-protocol/) v5 or above to connect.

If you try against an older version, you will get a `NotSupportedException` in some cases (e.g. prepared statements) and in other cases the driver will send the request anyway and the server will assume the previously set keyspace.

*Note: at the time of writing, Cassandra 4 is not released yet. If you want to test those examples against the development version, keep in mind that native protocol v5 is still in beta, so you'll need to allow beta protocol versions in the builder: `Builder.WithBetaProtocolVersions()`*.

## Basic usage

To use a per-query keyspace, set it on your statement instance:

```csharp
SimpleStatement statement =
    new SimpleStatement("SELECT * FROM foo WHERE k = 1").SetKeyspace("test");
session.Execute(statement);
```

You can do this on [simple](../simple/), [prepared](../prepared) or [batch](../batch/) statements.

If the session is connected to another keyspace, the per-query keyspace takes precedence:

```csharp
ISession session = Cluster.Builder().WithDefaultKeyspace("test1").Build().Connect();

// Will query test2.foo:
SimpleStatement statement =
    new SimpleStatement("SELECT * FROM foo WHERE k = 1").SetKeyspace("test2");
session.Execute(statement);
```

On the other hand, if a keyspace is hard-coded in the query, it takes precedence over the per-query keyspace:

```csharp
// Will query test1.foo:
SimpleStatement statement =
    new SimpleStatement("SELECT * FROM test1.foo WHERE k = 1").SetKeyspace("test2");
```

## Bound statements

Bound statements can't have a per-query keyspace; they only inherit the one that was set on the prepared statement:

```csharp
PreparedStatement pst = session.Prepare("SELECT * FROM foo WHERE k = ?", "test");

// Will query test.foo:
BoundStatement bs = pst.Bind(1);
```

The rationale is that prepared statements hold metadata about the target table; if Cassandra allowed execution against different keyspaces, it would be under the assumption that all tables have the same exact schema, which could create issues if this turned out not to be true at runtime.

Therefore you'll have to prepare against every target keyspace.

[CASSANDRA-10145]: https://issues.apache.org/jira/browse/CASSANDRA-10145