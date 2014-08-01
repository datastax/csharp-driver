using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Cassandra;
using CqlPoco.Mapping;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Creates statements for POCOs that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        private static readonly Regex SelectRegex = new Regex(@"\A\s*SELECT\s", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex FromRegex = new Regex(@"\A\s*FROM\s", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly PocoDataFactory _pocoDataFactory;

        public StatementFactory(PocoDataFactory pocoDataFactory)
        {
            if (pocoDataFactory == null) throw new ArgumentNullException("pocoDataFactory");
            _pocoDataFactory = pocoDataFactory;
        }

        public Task<IStatementWrapper> GetSelect<T>(string cql)
        {
            // Possibly add SELECT clause
            cql = AddSelectToCql<T>(cql);
            
            // TODO:  Cache/use prepared statements
            return Task.FromResult<IStatementWrapper>(new SimpleStatementWrapper(new SimpleStatement(cql)));
        }

        private string AddSelectToCql<T>(string cql)
        {
            // If it's already got a SELECT clause, just bail
            if (SelectRegex.IsMatch(cql))
                return cql;

            // Get the PocoData so we can generate a list of columns
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();
            string columns = string.Join(", ", pocoData.Columns.Keys);

            // If it's got the from clause, leave FROM intact, otherwise add it
            if (FromRegex.IsMatch(cql))
                return string.Format("SELECT {0} {1}", columns, cql);
            
            return string.Format("SELECT {0} FROM {1} {2}", columns, pocoData.TableName, cql);
        }
    }
}
