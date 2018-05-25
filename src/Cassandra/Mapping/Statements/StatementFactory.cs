using System;
using System.Collections.Concurrent;
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

        /// <summary>
        /// Given a <see cref="Cql"/>, it creates the corresponding <see cref="Statement"/>.
        /// </summary>
        /// <param name="session">The current session.</param>
        /// <param name="cql">The cql query, parameter and options.</param>
        /// <param name="forceNoPrepare">When defined, it's used to override the CQL options behavior.</param>
        public async Task<Statement> GetStatementAsync(ISession session, Cql cql, bool? forceNoPrepare = null)
        {
            var noPrepare = forceNoPrepare ?? cql.QueryOptions.NoPrepare;
            if (noPrepare)
            {
                // Use a SimpleStatement if we're not supposed to prepare
                var statement = new SimpleStatement(cql.Statement, cql.Arguments);
                SetStatementProperties(statement, cql);
                return statement;
            }

            var wasPreviouslyCached = true;

            var psCacheKey = cql.Statement;
            var query = cql.Statement;

            var prepareTask = _statementCache.GetOrAdd(psCacheKey, _ =>
            {
                wasPreviouslyCached = false;
                return session.PrepareAsync(query);
            });

            PreparedStatement ps;
            try
            {
                ps = await prepareTask.ConfigureAwait(false);
            }
            catch (Exception) when (wasPreviouslyCached)
            {
                // The exception was caused from awaiting upon a Task that was previously cached
                // It's possible that the schema or topology changed making this query preparation to succeed
                // in a new attemp
                prepareTask = session.PrepareAsync(query);
                ps = await prepareTask.ConfigureAwait(false);
                // AddOrUpdate() returns a task which we already waited upon, its safe to call Forget()
                _statementCache.AddOrUpdate(psCacheKey, prepareTask, (k, v) => prepareTask).Forget();
            }

            if (!wasPreviouslyCached)
            {
                var count = Interlocked.Increment(ref _statementCacheCount);
                if (count > MaxPreparedStatementsThreshold)
                {
                    Logger.Warning("The prepared statement cache contains {0} queries. This issue is probably due " +
                                   "to misuse of the driver, you should use parameter markers for queries. You can " +
                                   "configure this warning threshold using " +
                                   "MappingConfiguration.SetMaxStatementPreparedThreshold() method.", count);
                }
            }

            var boundStatement = ps.Bind(cql.Arguments);
            SetStatementProperties(boundStatement, cql);
            return boundStatement;
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

        public async Task<BatchStatement> GetBatchStatementAsync(ISession session, ICqlBatch cqlBatch)
        {
            // Get all the statements async in parallel, then add to batch
            var childStatements = await Task
                .WhenAll(cqlBatch.Statements.Select(cql => GetStatementAsync(session, cql, cqlBatch.Options.NoPrepare)))
                .ConfigureAwait(false);
            var statement = new BatchStatement().SetBatchType(cqlBatch.BatchType);
            cqlBatch.Options.CopyOptionsToStatement(statement);
            foreach (var stmt in childStatements)
            {
                statement.Add(stmt);
            }
            return statement;
        }
    }
}
