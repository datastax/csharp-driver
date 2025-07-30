# Speculative query execution

Sometimes a node might be experiencing difficulties (for example, long GC pause) and take longer than usual to reply. Queries sent to that node experience bad latency. One thing we can do to improve that is pre-emptively start a second execution of the query against another node, before the first node has replied or errored out. If that second node replies faster, we can send the response back to the client (we also cancel the first query):

```
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                |     host2 replies   |
  |                |<--------------------|
  |   complete     |              |
  |<---------------|              |
  |                | cancel       |
  |                |------------->|
```

Or the first node could reply just after the second execution was started. In this case, we cancel the second execution. In other words, whichever node replies faster wins and completes the client query:

```
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |              |      |
  |                | host1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

Speculative executions are disabled by default. The following sections cover the practical details and how to enable them.

## Query idempotence

One important aspect to consider is whether queries are idempotent, (that is, whether they can be applied multiple times without changing the result beyond the initial application). If a query is not idempotent, the driver never schedules speculative executions for it, because there is no way to guarantee that only one node will apply the mutation.

Examples of queries that are not idempotent are:

- counter operations
- prepending or appending to a list column
- using non-idempotent CQL functions, like `now()` or `uuid()`

In the driver, this is determined by [`Statement.IsIdempotent()`][isidempotent-api]. Because the driver does not parse query strings, in most cases, it has no information about what the query actually does. Therefore, for all other types of statements, it defaults to `false`. You must set it manually with one of the mechanisms described below.

You can override the value on each statement:

```csharp
var s = new SimpleStatement("SELECT * FROM users WHERE id = 1");
s.SetIdempotence(true);
```

Note: This also works for built statements (and it overrides the computed value). Additionally, if you know for a fact that your application does not use any of the non-idempotent CQL queries listed above, you can change the default cluster-wide:

```csharp
// Make all statements idempotent by default:
var cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithQueryOptions(new QueryOptions()
        .SetDefaultIdempotence(true))
    .Build();
```

## Enabling speculative execution

Speculative executions are controlled by an instance of `ISpeculativeExecutionPolicy` provided when initializing the
Cluster. This policy defines the threshold after which a new speculative execution is triggered.

The driver provides a `ConstantSpeculativeExecutionPolicy` that schedules a given number of speculative executions,
separated by a fixed delay.

This simple policy uses a constant threshold:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoint("127.0.0.1")
    .WithSpeculativeExecutionPolicy(
        new ConstantSpeculativeExecutionPolicy(
            500, // delay before a new execution is launched
            2    // maximum number of additional executions
        ))
    .Build();
```

Given the configuration above, an idempotent query would be handled this way:

- start the initial execution at t0
- if no response has been received at t0 + 500 milliseconds, start a speculative execution on another node
- if no response has been received at t0 + 1000 milliseconds, start another speculative execution on a third node

As with all policies, you are free to provide your own by implementing the `ISpeculativeExecutionPolicy` interface.

## How speculative executions affect retries

Regardless of speculative executions, the driver has a retry mechanism:

- on an internal error, it will try the next host
- if the consistency level cannot be reached (for example, unavailable error or read or write timeout), it delegates
the decision to the RetryPolicy, which might trigger a retry on the same host

Turning speculative executions on does not change this behavior. Each parallel execution trigger retries independently:

```
client           driver          exec1  exec2
--+----------------+--------------+------+---
  | execute(query) |
  |--------------->|
  |                | query host1
  |                |------------->|
  |                |              |
  |                | unavailable  |
  |                |<-------------|
  |                |
  |                |retry at lower CL
  |                |------------->|
  |                |              |
  |                |     query host2
  |                |-------------------->|
  |                |              |      |
  |                |     server error    |
  |                |<--------------------|
  |                |              |
  |                |   retry on host3
  |                |-------------------->|
  |                |              |      |
  |                | host1 replies|      |
  |                |<-------------|      |
  |   complete     |                     |
  |<---------------|                     |
  |                | cancel              |
  |                |-------------------->|
```

The only impact is that all executions of the same query always share the same query plan, so each host is used by at
most one execution.

## Tuning and practical details
 
The goal of speculative executions is to improve overall latency (the time between execute(query) and complete in the
diagrams above) at high percentiles. On the flipside, they cause the driver to send more individual requests, so
throughput does not necessarily improve.

One side-effect of speculative executions is that many requests are cancelled, which can lead to a phenomenon called
stream id exhaustion: each TCP connection can handle multiple simultaneous requests, identified by a unique
number called stream id. When a request gets cancelled, we can't reuse its stream id immediately because we might
still receive a response from the server later. If this happens often, the number of available stream ids diminishes
over time, and when it goes below a given threshold we close the connection and create a new one. If requests are often
cancelled, so will see connections being recycled at a high rate.

This problem is more likely to happen with Cassandra version 2.0 or below (version 1 or 2 of the native protocol),
because each TCP connection only has 128 stream ids. With following versions of Cassandra, there are 32K stream ids per
connection, so higher cancellation rates can be sustained.

Another issue that might arise is that you get unintuitive results because of request ordering. Suppose you run the
following query with speculative executions enabled:

```
insert into my_table (k, v) values (1, 1);
```

The first execution is a bit too slow, so a second execution gets triggered. Finally, the first execution completes,
so the client code gets back an acknowledgement, and the second execution is cancelled. However, cancelling only means
that the driver stops waiting for the server's response, the request could still be on the wire; let us assume that
this is the case. Now you run the following query, which completes successfully:

```
delete from my_table where k = 1;
```

But now the second execution of the first query finally reaches its target node, which applies the mutation. The row
that you've just deleted is back! The workaround is to use a timestamp with your queries:

```
insert into my_table (k, v) values (1, 1) USING TIMESTAMP 1432764000;
```

If you're using Cassandra 2.1 and above, you can use client-side timestamps with the method
[Statement.SetTimestamp()][settimestamp-api].

[isidempotent-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.IStatement.html#Cassandra_IStatement_IsIdempotent
[settimestamp-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.Statement.html#Cassandra_Statement_SetTimestamp_System_DateTimeOffset_