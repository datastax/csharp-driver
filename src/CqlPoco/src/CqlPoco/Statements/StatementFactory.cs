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
        private readonly PocoData _pocoData;

        public StatementFactory(PocoData pocoData)
        {
            if (pocoData == null) throw new ArgumentNullException("pocoData");
            _pocoData = pocoData;
        }

        public Task<IStatementWrapper> GetSelect(string cql)
        {
            // TODO:  Cache/use prepared statements, generate SELECT clause automagically
            return Task.FromResult<IStatementWrapper>(new SimpleStatementWrapper(new SimpleStatement(cql)));
        }
    }
}
