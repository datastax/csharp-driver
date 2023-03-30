# Statements

## Quick overview

Statements are what you pass to `Session.Execute()` and `Session.ExecuteAsync()`.

There are three types:

* [SimpleStatement](simple/): a simple implementation built directly from a character string. 
  Typically used for queries that are executed only once or a few times.
* [BoundStatement (from PreparedStatement)](prepared/): obtained by binding values to a prepared
  query. Typically used for queries that are executed often, with different values.
* [BatchStatement](batch/): a statement that groups multiple statements to be executed as a batch.

All statement types share a common set of execution attributes, that can be set through setters:

* [execution profile](../../../execution-profiles/) name.
* idempotent flag.
* tracing flag.
* [query timestamp](../../../query-timestamps/).
* [page size and paging state](../../../paging/).
* [per-query keyspace](per_query_keyspace/) (Cassandra 4 or above).
* [token-aware routing](../../../routing-queries) information (keyspace and key/token).
* normal and serial consistency level.
* read timeout.
* custom payload to send arbitrary key/value pairs with the request (you should only need this if you have a custom query handler on the server).

Note that some attributes can either be set programmatically, or inherit a default value defined in the `Builder`. We recommended setting these values in the `Builder` whenever possible (you
can create execution profiles to capture common combinations of those options).
