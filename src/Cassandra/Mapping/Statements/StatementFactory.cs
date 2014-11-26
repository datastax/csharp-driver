using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cassandra.Mapping.Statements
{
    /// <summary>
    /// Creates statements from CQL that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        private readonly ISession _session;

        private readonly ConcurrentDictionary<string, Task<PreparedStatement>> _statementCache;

        public StatementFactory(ISession session)
        {
            _session = session;
            _statementCache = new ConcurrentDictionary<string, Task<PreparedStatement>>();
        }

        public Task<Statement> GetStatementAsync(Cql cql)
        {
            // Use a SimpleStatement if we're not supposed to prepare
            if (cql.QueryOptions.NoPrepare)
            {
                Statement statement = new SimpleStatement(cql.Statement).Bind(cql.Arguments);
                cql.QueryOptions.CopyOptionsToStatement(statement);
                return TaskHelper.ToTask(statement);
            }
            return _statementCache
                .GetOrAdd(cql.Statement, _session.PrepareAsync)
                .Continue(t =>
                {
                    var boundStatement = t.Result.Bind(cql.Arguments);
                    cql.QueryOptions.CopyOptionsToStatement(boundStatement);
                    return (Statement)boundStatement;
                });
        }

        public Statement GetStatement(Cql cql)
        {
            // Just use async version's result
            return GetStatementAsync(cql).Result;
        }

        public Task<BatchStatement> GetBatchStatementAsync(IEnumerable<Cql> cqlToBatch)
        {
            // Get all the statements async in parallel, then add to batch
            return Task.Factory.ContinueWhenAll(cqlToBatch.Select(GetStatementAsync).ToArray(), (tasks) =>
            {
                var batch = new BatchStatement();
                foreach (var t in tasks)
                {
                    if (t.Exception != null)
                    {
                        throw t.Exception;
                    }
                    batch.Add(t.Result);
                }
                return batch;
            }, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        public BatchStatement GetBatchStatement(IEnumerable<Cql> cqlToBatch)
        {
            var batch = new BatchStatement();
            foreach (Cql cql in cqlToBatch)
            {
                batch.Add(GetStatement(cql));
            }
            return batch;
        }
    }
}
