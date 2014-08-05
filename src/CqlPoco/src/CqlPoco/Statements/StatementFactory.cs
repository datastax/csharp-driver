using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Cassandra;
using CqlPoco.Mapping;
using CqlPoco.Utils;

namespace CqlPoco.Statements
{
    /// <summary>
    /// Creates statements for POCOs that can be executed with the C* driver.
    /// </summary>
    internal class StatementFactory
    {
        private const string CannotGenerateStatementForPoco = "Cannot create {0} statement for POCO of type {1}";
        private const string NoColumns = CannotGenerateStatementForPoco + " because it has no columns";

        private const string MissingPkColumns = CannotGenerateStatementForPoco + " because it is missing PK columns {2}.  " +
                                                "Are you missing a property/field on the POCO or did you forget to specify " +
                                                "the PK columns in the mapping?";

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

        public Task<IStatementWrapper> GetInsert<T>()
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0)
                throw new InvalidOperationException(string.Format(NoColumns, "INSERT", typeof(T).Name));

            string columns = pocoData.Columns.Select(c => c.ColumnName).ToCommaDelimitedString();
            string placeholders = Enumerable.Repeat("?", pocoData.Columns.Count).ToCommaDelimitedString();

            string cql = string.Format("INSERT INTO {0} ({1}) VALUES ({2})", pocoData.TableName, columns, placeholders);

            // TODO:  Cache/use prepared statements
            return Task.FromResult<IStatementWrapper>(new SimpleStatementWrapper(new SimpleStatement(cql)));
        }

        public Task<IStatementWrapper> GetUpdate<T>()
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0) 
                throw new InvalidOperationException(string.Format(NoColumns, "UPDATE", typeof(T).Name));

            if (pocoData.MissingPrimaryKeyColumns.Length > 0)
            {
                throw new InvalidOperationException(string.Format(MissingPkColumns, "UPDATE", typeof(T).Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }

            string nonPkColumns = pocoData.GetNonPrimaryKeyColumns().Select(c => string.Format("{0} = ?", c.ColumnName)).ToCommaDelimitedString();
            string pkColumns = string.Join(" AND ", pocoData.GetPrimaryKeyColumns().Select(c => string.Format("{0} = ?", c.ColumnName)));

            string cql = string.Format("UPDATE {0} SET {1} WHERE {2}", pocoData.TableName, nonPkColumns, pkColumns);

            // TODO: Cache/use prepared statements
            return Task.FromResult<IStatementWrapper>(new SimpleStatementWrapper(new SimpleStatement(cql)));
        }

        public Task<IStatementWrapper> GetDelete<T>()
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0)
                throw new InvalidOperationException(string.Format(NoColumns, "DELETE", typeof(T).Name));

            if (pocoData.MissingPrimaryKeyColumns.Length > 0)
            {
                throw new InvalidOperationException(string.Format(MissingPkColumns, "DELETE", typeof(T).Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }
                

            string pkColumns = string.Join(" AND ", pocoData.GetPrimaryKeyColumns().Select(c => string.Format("{0} = ?", c.ColumnName)));

            string cql = string.Format("DELETE FROM {0} WHERE {1}", pocoData.TableName, pkColumns);

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
            string allColumns = pocoData.Columns.Select(c => c.ColumnName).ToCommaDelimitedString();

            // If it's got the from clause, leave FROM intact, otherwise add it
            if (FromRegex.IsMatch(cql))
                return string.Format("SELECT {0} {1}", allColumns, cql);
            
            return string.Format("SELECT {0} FROM {1} {2}", allColumns, pocoData.TableName, cql);
        }
    }
}
