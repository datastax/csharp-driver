using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Cassandra.Mapping.Mapping;
using Cassandra.Mapping.Utils;

namespace Cassandra.Mapping.Statements
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
            var pocoData = _pocoDataFactory.GetPocoData<T>();
            var allColumns = pocoData.Columns.Select(Escape(pocoData)).ToCommaDelimitedString();

            // If it's got the from clause, leave FROM intact, otherwise add it
            cql.SetStatement(FromRegex.IsMatch(cql.Statement)
                                 ? string.Format("SELECT {0} {1}", allColumns, cql.Statement)
                                 : string.Format("SELECT {0} FROM {1} {2}", allColumns, Escape(pocoData.TableName, pocoData), cql.Statement));
        }

        /// <summary>
        /// Escapes an identier if necessary
        /// </summary>
        private static string Escape(string identifier, PocoData pocoData)
        {
            if (!pocoData.CaseSensitive)
            {
                return identifier;
            }
            return "\"" + identifier + "\"";
        }

        private static Func<PocoColumn, string> Escape(PocoData pocoData)
        {
            Func<PocoColumn, string> f = (c) => Escape(c.ColumnName, pocoData);
            return f;
        }

        private static Func<PocoColumn, string> Escape(PocoData pocoData, string format)
        {
            Func<PocoColumn, string> f = (c) => String.Format(format, Escape(c.ColumnName, pocoData));
            return f;
        }

        /// <summary>
        /// Generates an "INSERT INTO tablename (columns) VALUES (?)" statement for a POCO of Type T.
        /// </summary>
        public string GenerateInsert<T>()
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0)
                throw new InvalidOperationException(string.Format(NoColumns, "INSERT", typeof(T).Name));

            var columns = pocoData.Columns.Select(Escape(pocoData)).ToCommaDelimitedString();
            var placeholders = Enumerable.Repeat("?", pocoData.Columns.Count).ToCommaDelimitedString();
            return string.Format("INSERT INTO {0} ({1}) VALUES ({2})", Escape(pocoData.TableName, pocoData), columns, placeholders);
        }
        
        /// <summary>
        /// Generates an "UPDATE tablename SET columns = ? WHERE pkColumns = ?" statement for a POCO of Type T.
        /// </summary>
        public string GenerateUpdate<T>()
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0)
                throw new InvalidOperationException(string.Format(NoColumns, "UPDATE", typeof(T).Name));

            if (pocoData.MissingPrimaryKeyColumns.Count > 0)
            {
                throw new InvalidOperationException(string.Format(MissingPkColumns, "UPDATE", typeof(T).Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }

            var nonPkColumns = pocoData.GetNonPrimaryKeyColumns().Select(Escape(pocoData, "{0} = ?")).ToCommaDelimitedString();
            var pkColumns = string.Join(" AND ", pocoData.GetPrimaryKeyColumns().Select(Escape(pocoData, "{0} = ?")));
            return string.Format("UPDATE {0} SET {1} WHERE {2}", Escape(pocoData.TableName, pocoData), nonPkColumns, pkColumns);
        }

        /// <summary>
        /// Prepends the CQL statement specified with "UPDATE tablename " for a POCO of Type T.
        /// </summary>
        public void PrependUpdate<T>(Cql cql)
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement(string.Format("UPDATE {0} {1}", Escape(pocoData.TableName, pocoData), cql.Statement));
        }

        /// <summary>
        /// Generates a "DELETE FROM tablename WHERE pkcolumns = ?" statement for a POCO of Type T.
        /// </summary>
        public string GenerateDelete<T>()
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0)
            {
                throw new InvalidOperationException(string.Format(NoColumns, "DELETE", typeof(T).Name));
            }

            if (pocoData.MissingPrimaryKeyColumns.Count > 0)
            {
                throw new InvalidOperationException(string.Format(MissingPkColumns, "DELETE", typeof(T).Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }

            var pkColumns = String.Join(" AND ", pocoData.GetPrimaryKeyColumns().Select(Escape(pocoData, "{0} = ?")));
            return string.Format("DELETE FROM {0} WHERE {1}", Escape(pocoData.TableName, pocoData), pkColumns);
        }

        /// <summary>
        /// Prepends the CQL statement specified with "DELETE FROM tablename " for a POCO of Type T.
        /// </summary>
        public void PrependDelete<T>(Cql cql)
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement(string.Format("DELETE FROM {0} {1}", pocoData.TableName, cql.Statement));
        }

        private static string GetTypeString(PocoColumn column)
        {
            IColumnInfo typeInfo;
            var typeCode = TypeCodec.GetColumnTypeCodeInfo(column.ColumnType, out typeInfo);
            return GetTypeString(typeCode, typeInfo);
        }

        /// <summary>
        /// Gets the CQL queries involved in a table creation (CREATE TABLE, CREATE INDEX)
        /// </summary>
        public static List<string> GetCreate(PocoData pocoData, bool ifNotExists)
        {
            if (pocoData == null)
            {
                throw new ArgumentNullException("pocoData");
            }
            if (pocoData.MissingPrimaryKeyColumns.Count > 0)
            {
                throw new InvalidOperationException(string.Format(MissingPkColumns, "CREATE", pocoData.PocoType.Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }
            var commands = new List<string>();
            var createTable = new StringBuilder("CREATE TABLE ");
            createTable.Append(Escape(pocoData.TableName, pocoData));
            createTable.Append(" (");
            foreach (var column in pocoData.Columns)
            {
                createTable
                    .Append(Escape(column.ColumnName, pocoData))
                    .Append(" ");
                //TODO: Counter columns
                createTable    
                    .Append(GetTypeString(column))
                    .Append(", ");
            }
            createTable.Append("PRIMARY KEY (");
            if (pocoData.PartitionKeys.Count == 1)
            {
                createTable.Append(Escape(pocoData.PartitionKeys[0].ColumnName, pocoData));
            }
            else
            {
                //tupled partition keys
                createTable
                    .Append("(")
                    .Append(String.Join(", ", pocoData.PartitionKeys.Select(Escape(pocoData))))
                    .Append(")");
            }
            if (pocoData.ClusteringKeys.Count > 0)
            {
                createTable.Append(", ");
                createTable.Append(String.Join(", ", pocoData.ClusteringKeys.Select(k => Escape(k.Item1.ColumnName, pocoData))));
            }
            //close primary keys
            createTable.Append(")");
            //close table column definition
            createTable.Append(")");
            var clusteringOrder = String.Join(", ", pocoData.ClusteringKeys
                .Where(k => k.Item2 != SortOrder.Unspecified)
                .Select(k => Escape(k.Item1.ColumnName, pocoData) + " " + (k.Item2 == SortOrder.Ascending ? "ASC" : "DESC")));

            if (!String.IsNullOrEmpty(clusteringOrder))
            {
                createTable
                    .Append(" WITH CLUSTERING ORDER BY (")
                    .Append(clusteringOrder)
                    .Append(")");
            }
            //TODO: Compact storage
            commands.Add(createTable.ToString());
            //TODO: Secondary indexes
            return commands;
        }

        private static string GetTypeString(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                //Is a single type
                return typeCode.ToString().ToLower();
            }
            if (typeInfo is MapColumnInfo)
            {
                var mapInfo = typeInfo as MapColumnInfo;
                return "map<" +
                       GetTypeString(mapInfo.KeyTypeCode, mapInfo.KeyTypeInfo) +
                       ", " +
                       GetTypeString(mapInfo.ValueTypeCode, mapInfo.ValueTypeInfo) +
                       ">";
            }
            if (typeInfo is SetColumnInfo)
            {
                var setInfo = typeInfo as SetColumnInfo;
                return "set<" +
                       GetTypeString(setInfo.KeyTypeCode, setInfo.KeyTypeInfo) +
                       ">";
            }
            if (typeInfo is ListColumnInfo)
            {
                var setInfo = typeInfo as ListColumnInfo;
                return "list<" +
                       GetTypeString(setInfo.ValueTypeCode, setInfo.ValueTypeInfo) +
                       ">";
            }
            if (typeInfo is TupleColumnInfo)
            {
                var tupleInfo = typeInfo as TupleColumnInfo;
                return "tuple<" +
                       String.Join(", ", tupleInfo.Elements.Select(e => GetTypeString(e.TypeCode, e.TypeInfo))) +
                       ">";
            }
            if (typeInfo is UdtColumnInfo)
            {
                var udtInfo = typeInfo as UdtColumnInfo;
                return udtInfo.Name;
            }
            throw new NotSupportedException(String.Format("Type {0} is not suppoted", typeCode));
        }
    }
}