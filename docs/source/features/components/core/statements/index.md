# Statements

## Quick overview

Statements are what you pass to `Session.Execute()` and `Session.ExecuteAsync()`.

There are three types:

* [SimpleStatement](simple/index): a simple implementation built directly from a character string. 
  Typically used for queries that are executed only once or a few times.
* [BoundStatement (from PreparedStatement)](prepared/index): obtained by binding values to a prepared
  query. Typically used for queries that are executed often, with different values.
* [BatchStatement](batch/index): a statement that groups multiple statements to be executed as a batch.

All statement types share a common set of execution attributes, that can be set through setters:

* [execution profile](../../../execution-profiles/index) name.
* idempotent flag.
* tracing flag.
* [query timestamp](../../../query-timestamps/index).
* [page size and paging state](../../../paging/index).
* [per-query keyspace](per-query-keyspace/index) (Cassandra 4 or above).
* [token-aware routing](../../../routing-queries/index) information (keyspace and key/token).
* normal and serial consistency level.
* read timeout.
* custom payload to send arbitrary key/value pairs with the request (you should only need this if you have a custom query handler on the server).

Note that some attributes can either be set programmatically, or inherit a default value defined in the `Builder`. We recommended setting these values in the `Builder` whenever possible (you
can create execution profiles to capture common combinations of those options).

:::{toctree}
:hidden:
:maxdepth: 1

batch/index
per-query-keyspace/index
prepared/index
simple/index
:::
