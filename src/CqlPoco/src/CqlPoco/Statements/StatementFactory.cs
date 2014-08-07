using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Creates statements from CQL that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        public Task<Statement> GetStatementAsync(Cql cql)
        {
            // TODO:  Cache/use prepared statements
            var statement = new SimpleStatement(cql.Statement).Bind(cql.Arguments);
            cql.QueryOptions.CopyOptionsToStatement(statement);
            return Task.FromResult<Statement>(statement);
        }

        public Statement GetStatement(Cql cql)
        {
            var statement = new SimpleStatement(cql.Statement).Bind(cql.Arguments);
            cql.QueryOptions.CopyOptionsToStatement(statement);
            return statement;
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
