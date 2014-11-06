using System;
using System.Linq;
using System.Text.RegularExpressions;
using CqlPoco.Mapping;
using CqlPoco.Utils;

namespace CqlPoco.Statements
{
    /// <summary>
    /// A utility class capable of generating CQL statements for a POCO.
    /// </summary>
    internal class CqlGenerator
    {
        private const string CannotGenerateStatementForPoco = "Cannot create {0} statement for POCO of type {1}";
        private const string NoColumns = CannotGenerateStatementForPoco + " because it has no columns";

        private const string MissingPkColumns = CannotGenerateStatementForPoco + " because it is missing PK columns {2}.  " +
                                                "Are you missing a property/field on the POCO or did you forget to specify " +
                                                "the PK columns in the mapping?";

        private static readonly Regex SelectRegex = new Regex(@"\A\s*SELECT\s", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex FromRegex = new Regex(@"\A\s*FROM\s", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly PocoDataFactory _pocoDataFactory;

        public CqlGenerator(PocoDataFactory pocoDataFactory)
        {
            if (pocoDataFactory == null) throw new ArgumentNullException("pocoDataFactory");
            _pocoDataFactory = pocoDataFactory;
        }

        /// <summary>
        /// Adds "SELECT columnlist" and "FROM tablename" to a CQL statement if they don't already exist for a POCO of Type T.
        /// </summary>
        public void AddSelect<T>(Cql cql)
        {
            // If it's already got a SELECT clause, just bail
            if (SelectRegex.IsMatch(cql.Statement))
                return;

            // Get the PocoData so we can generate a list of columns
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();
            string allColumns = pocoData.Columns.Select(c => c.ColumnName).ToCommaDelimitedString();

            // If it's got the from clause, leave FROM intact, otherwise add it
            cql.SetStatement(FromRegex.IsMatch(cql.Statement)
                                 ? string.Format("SELECT {0} {1}", allColumns, cql.Statement)
                                 : string.Format("SELECT {0} FROM {1} {2}", allColumns, pocoData.TableName, cql.Statement));
        }

        /// <summary>
        /// Generates an "INSERT INTO tablename (columns) VALUES (?)" statement for a POCO of Type T.
        /// </summary>
        public string GenerateInsert<T>()
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0)
                throw new InvalidOperationException(string.Format(NoColumns, "INSERT", typeof(T).Name));

            string columns = pocoData.Columns.Select(c => c.ColumnName).ToCommaDelimitedString();
            string placeholders = Enumerable.Repeat("?", pocoData.Columns.Count).ToCommaDelimitedString();
            return string.Format("INSERT INTO {0} ({1}) VALUES ({2})", pocoData.TableName, columns, placeholders);
        }
        
        /// <summary>
        /// Generates an "UPDATE tablename SET columns = ? WHERE pkColumns = ?" statement for a POCO of Type T.
        /// </summary>
        public string GenerateUpdate<T>()
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
            return string.Format("UPDATE {0} SET {1} WHERE {2}", pocoData.TableName, nonPkColumns, pkColumns);
        }

        /// <summary>
        /// Prepends the CQL statement specified with "UPDATE tablename " for a POCO of Type T.
        /// </summary>
        public void PrependUpdate<T>(Cql cql)
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement(string.Format("UPDATE {0} {1}", pocoData.TableName, cql.Statement));
        }

        /// <summary>
        /// Generates a "DELETE FROM tablename WHERE pkcolumns = ?" statement for a POCO of Type T.
        /// </summary>
        public string GenerateDelete<T>()
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
            return string.Format("DELETE FROM {0} WHERE {1}", pocoData.TableName, pkColumns);
        }

        /// <summary>
        /// Prepends the CQL statement specified with "DELETE FROM tablename " for a POCO of Type T.
        /// </summary>
        public void PrependDelete<T>(Cql cql)
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement(string.Format("DELETE FROM {0} {1}", pocoData.TableName, cql.Statement));
        }
    }
}