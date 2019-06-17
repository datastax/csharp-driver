# C# Driver usage samples

This folder contains examples on how to use some features of the C# Driver for [Apache Cassandra][cassandra].

You should also visit the [Documentation][doc-index] and [FAQ][faq].

## Code samples
- Concurrent execution
  - [Inserts multiple rows in a table limiting the amount of parallel requests](ConcurrentExecutions/ExecuteInLoop/Program.cs)

Each example is generally structured in a way where the `Cluster` and `Session` are connected at the beginning and shutdown/disposed at the end.
While this is suitable for example console applications, you should reuse the `Session` and `Cluster` instances and only call `cluster.ShutdownAsync()` / `cluster.Dispose()` when exiting your application.

If you have any questions regarding these examples, feel free to post your questions in the [mailing list][mailing-list].

[cassandra]: https://cassandra.apache.org/
[doc-index]: https://docs.datastax.com/en/developer/csharp-driver/latest/
[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user
[faq]: https://docs.datastax.com/en/developer/csharp-driver/latest/faq/
