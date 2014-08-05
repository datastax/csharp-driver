using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Creates statements for POCOs that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        public Task<Statement> GetStatementAsync(string cql, params object[] args)
        {
            // TODO:  Cache/use prepared statements
            return Task.FromResult<Statement>(new SimpleStatement(cql).Bind(args));
        }

        public Statement GetStatement(string cql, params object[] args)
        {
            return new SimpleStatement(cql).Bind(args);
        }

        public async Task<BatchStatement> GetBatchStatementAsync(IEnumerable<Cql> cqlToBatch)
        {
            // Get all the statements async in parallel, then add to batch
            Statement[] statements = await Task.WhenAll(cqlToBatch.Select(cql => GetStatementAsync(cql.Statement, cql.Arguments)));

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
                batch.Add(GetStatement(cql.Statement, cql.Arguments));
            }
            return batch;
        }
    }
}
