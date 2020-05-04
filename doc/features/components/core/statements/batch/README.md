# Batch statements

Use [BatchStatement] to execute a set of queries as an atomic operation (refer to [Batching inserts, updates and deletes][batch_dse] to understand how to use batching effectively):

```csharp
PreparedStatement preparedInsertExpense =
    session.Prepare(
        "INSERT INTO cyclist_expenses (cyclist_name, expense_id, amount, description, paid) "
            + "VALUES (:name, :id, :amount, :description, :paid)");
SimpleStatement simpleInsertBalance =
    new SimpleStatement(
        "INSERT INTO cyclist_expenses (cyclist_name, balance) VALUES (?, 0) IF NOT EXISTS",
        "Vera ADRIAN");

BatchStatement batch =
  new BatchStatement()
      .SetBatchType(BatchType.Logged)
      .Add(simpleInsertBalance)
      .Add(preparedInsertExpense.bind("Vera ADRIAN", 1, 7.95f, "Breakfast", false));

session.Execute(batch);
```

As shown in the examples above, batches can contain any combination of simple statements and bound statements. A given batch can contain at most 65536 statements. Past this limit, addition methods throw an `ArgumentOutOfRangeException`.

In addition, simple statements with named parameters are currently not supported in batches (this is due to a [protocol limitation][CASSANDRA-10246] that will be fixed in a future version).

[batch_dse]: http://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useBatch.html
[CASSANDRA-10246]: https://issues.apache.org/jira/browse/CASSANDRA-10246
