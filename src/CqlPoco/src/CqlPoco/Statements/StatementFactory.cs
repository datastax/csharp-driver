using System.Threading.Tasks;
using Cassandra;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Creates statements for POCOs that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        public Task<IStatement> GetStatement(string cql, params object[] args)
        {
            // TODO:  Cache/use prepared statements
            return Task.FromResult<IStatement>(new SimpleStatement(cql).Bind(args));
        }
    }
}
