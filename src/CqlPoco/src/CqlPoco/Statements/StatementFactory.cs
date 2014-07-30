using System;
using System.Threading.Tasks;
using Cassandra;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Creates statements for POCOs that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        public StatementFactory()
        {
        }

        public Task<IStatementWrapper> GetSelect(string cql)
        {
            // TODO:  Cache/use prepared statements, generate SELECT clause automagically
            return Task.FromResult<IStatementWrapper>(new SimpleStatementWrapper(new SimpleStatement(cql)));
        }
    }
}
