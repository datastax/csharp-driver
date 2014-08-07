using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using CqlPoco.Utils;

namespace CqlPoco.Statements
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
            if (session == null) throw new ArgumentNullException("session");
            _session = session;

            _statementCache = new ConcurrentDictionary<string, Task<PreparedStatement>>();
        }

        public async Task<Statement> GetStatementAsync(Cql cql)
        {
            // Use a SimpleStatement if we're not supposed to prepare
            if (cql.QueryOptions.NoPrepare)
            {
                var statement = new SimpleStatement(cql.Statement).Bind(cql.Arguments);
                cql.QueryOptions.CopyOptionsToStatement(statement);
                return statement;
            }

            PreparedStatement preparedStatement = await _statementCache.GetOrAdd(cql.Statement, _session.PrepareAsync).ConfigureAwait(false);
            BoundStatement boundStatement = preparedStatement.Bind(cql.Arguments);
            cql.QueryOptions.CopyOptionsToStatement(boundStatement);
            return boundStatement;
        }

        public Statement GetStatement(Cql cql)
        {
            // Just use async version's result
            return GetStatementAsync(cql).Result;
        }

        public async Task<BatchStatement> GetBatchStatementAsync(IEnumerable<Cql> cqlToBatch)
        {
            // Get all the statements async in parallel, then add to batch
            Statement[] statements = await Task.WhenAll(cqlToBatch.Select(GetStatementAsync));

            var batch = new BatchStatement();
            foreach (var statement in statements)
            {
                batch.Add(statement);
            }
            return batch;
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
