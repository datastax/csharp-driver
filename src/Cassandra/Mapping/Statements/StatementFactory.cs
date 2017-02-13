using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra.Mapping.Statements
{
    /// <summary>
    /// Creates statements from CQL that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        private readonly ConcurrentDictionary<string, Task<PreparedStatement>> _statementCache;
        private static readonly Logger Logger = new Logger(typeof(StatementFactory));
        private int _statementCacheCount;

        public int MaxPreparedStatementsThreshold { get; set; }

        public StatementFactory()
        {
            MaxPreparedStatementsThreshold = 500;
            _statementCache = new ConcurrentDictionary<string, Task<PreparedStatement>>();
        }

        public async Task<Statement> GetStatementAsync(ISession session, Cql cql)
        {
            if (cql.QueryOptions.NoPrepare)
            {
                // Use a SimpleStatement if we're not supposed to prepare
                var statement = new SimpleStatement(cql.Statement, cql.Arguments);
                SetStatementProperties(statement, cql);
                return statement;
            }
            var ps = await _statementCache
                .GetOrAdd(cql.Statement, query => PrepareNew(query, session))
                .ConfigureAwait(false);
            var boundStatement = ps.Bind(cql.Arguments);
            SetStatementProperties(boundStatement, cql);
            return boundStatement;
        }

        private Task<PreparedStatement> PrepareNew(string query, ISession session)
        {
            var count = Interlocked.Increment(ref _statementCacheCount);
            if (count > MaxPreparedStatementsThreshold)
            {
                Logger.Warning("The prepared statement cache contains {0} queries. This issue is probably due " +
                               "to misuse of the driver, you should use parameter markers for queries. You can " +
                               "configure this warning threshold using " +
                               "MappingConfiguration.SetMaxStatementPreparedThreshold() method.", count);
            }
            return session.PrepareAsync(query);
        }

        private void SetStatementProperties(IStatement stmt, Cql cql)
        {
            cql.QueryOptions.CopyOptionsToStatement(stmt);
            stmt.SetAutoPage(cql.AutoPage);
        }

        public Statement GetStatement(ISession session, Cql cql)
        {
            // Just use async version's result
            return GetStatementAsync(session, cql).Result;
        }

        public Task<BatchStatement> GetBatchStatementAsync(ISession session, IEnumerable<Cql> cqlToBatch, BatchType batchType)
        {
            // Get all the statements async in parallel, then add to batch
            return Task.Factory.ContinueWhenAll(cqlToBatch.Select(cql => GetStatementAsync(session, cql)).ToArray(), (tasks) =>
            {
                var batch = new BatchStatement().SetBatchType(batchType);
                foreach (var t in tasks)
                {
                    if (t.Exception != null)
                    {
                        throw t.Exception;
                    }
                    batch.Add(t.Result);
                }
                return batch;
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        public BatchStatement GetBatchStatement(ISession session, IEnumerable<Cql> cqlToBatch, BatchType batchType)
        {
            var batch = new BatchStatement().SetBatchType(batchType);
            foreach (var cql in cqlToBatch)
            {
                batch.Add(GetStatement(session, cql));
            }
            return batch;
        }
    }
}
