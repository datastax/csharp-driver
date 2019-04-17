using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Cassandra.Mapping.Utils;
using Cassandra.Serialization;

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
        private static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

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

            var suffix = cql.Statement == string.Empty ? string.Empty : " " + cql.Statement;

            // If it's got the from clause, leave FROM intact, otherwise add it
            cql.SetStatement(FromRegex.IsMatch(cql.Statement)
                                 ? string.Format("SELECT {0}{1}", allColumns, suffix)
                                 : string.Format("SELECT {0} FROM {1}{2}", allColumns, GetEscapedTableName(pocoData), suffix));
        }

        private static string GetEscapedTableName(PocoData pocoData)
        {
            string name = null;
            if (!string.IsNullOrEmpty(pocoData.KeyspaceName))
            {
                name = Escape(pocoData.KeyspaceName, pocoData) + ".";
            }
            name += Escape(pocoData.TableName, pocoData);
            return name;
        }

        /// <summary>
        /// Escapes an identier if necessary
        /// </summary>
        private static string Escape(string identifier, PocoData pocoData)
        {
            if (!pocoData.CaseSensitive && !string.IsNullOrWhiteSpace(identifier))
            {
                return identifier;
            }
            return "\"" + identifier + "\"";
        }

        private static Func<PocoColumn, string> Escape(PocoData pocoData)
        {
            Func<PocoColumn, string> f = c => Escape(c.ColumnName, pocoData);
            return f;
        }

        private static Func<PocoColumn, string> Escape(PocoData pocoData, string format)
        {
            Func<PocoColumn, string> f = c => String.Format(format, Escape(c.ColumnName, pocoData));
            return f;
        }

        /// <summary>
        /// Generates an "INSERT INTO tablename (columns) VALUES (?...)" statement for a POCO of Type T.
        /// </summary>
        /// <param name="insertNulls">When set to <c>true</c>, it will only generate columns which POCO members are not null</param>
        /// <param name="pocoValues">The parameters of this query, it will only be used if <c>insertNulls</c> is set to <c>true</c></param>
        /// <param name="queryParameters">The parameters for this query. When insertNulls is <c>true</c>, the <c>pocoValues</c>
        /// is the <c>queryParameters</c>, when set to <c>false</c> the <c>queryParameters do not include <c>null</c> values</c></param>
        /// <param name="ifNotExists">Determines if it should add the IF NOT EXISTS at the end of the query</param>
        /// <param name="ttl">Amount of seconds for the data to expire (TTL)</param>
        /// <param name="timestamp">Data timestamp</param>
        /// <param name="tableName">Table name. If null, it will use table name based on Poco Data</param>
        /// <returns></returns>
        public string GenerateInsert<T>(bool insertNulls, object[] pocoValues, out object[] queryParameters, 
            bool ifNotExists = false, int? ttl = null, DateTimeOffset? timestamp = null, string tableName = null)
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();
            if (pocoData.Columns.Count == 0)
            {
                throw new InvalidOperationException(string.Format(NoColumns, "INSERT", typeof(T).Name));
            }
            string columns;
            string placeholders;
            var parameterList = new List<object>();
            if (!insertNulls)
            {
                if (pocoValues == null)
                {
                    throw new ArgumentNullException("pocoValues");
                }
                if (pocoValues.Length != pocoData.Columns.Count)
                {
                    throw new ArgumentException("Values array should contain the same amount of items as POCO columns");
                }
                //only include columns which value is not null
                var columnsBuilder = new StringBuilder();
                var placeholdersBuilder = new StringBuilder();
                var encounteredNonNull = false;
                for (var i = 0; i < pocoData.Columns.Count; i++)
                {
                    var value = pocoValues[i];
                    if (value == null)
                    {
                        continue;
                    }
                    if (encounteredNonNull)
                    {
                        columnsBuilder.Append(", ");
                        placeholdersBuilder.Append(", ");
                    }
                    encounteredNonNull = true;
                    columnsBuilder.Append(Escape(pocoData)(pocoData.Columns[i]));
                    placeholdersBuilder.Append("?");
                    parameterList.Add(value);
                }
                columns = columnsBuilder.ToString();
                placeholders = placeholdersBuilder.ToString();
            }
            else
            {
                //Include all columns defined in the Poco
                columns = pocoData.Columns.Select(Escape(pocoData)).ToCommaDelimitedString();
                placeholders = Enumerable.Repeat("?", pocoData.Columns.Count).ToCommaDelimitedString();
                parameterList.AddRange(pocoValues);
            }
            var queryBuilder = new StringBuilder();
            queryBuilder.Append("INSERT INTO ");
            queryBuilder.Append(tableName ?? GetEscapedTableName(pocoData));
            queryBuilder.Append(" (");
            queryBuilder.Append(columns);
            queryBuilder.Append(") VALUES (");
            queryBuilder.Append(placeholders);
            queryBuilder.Append(")");
            if (ifNotExists)
            {
                queryBuilder.Append(" IF NOT EXISTS");
            }
            if (ttl != null || timestamp != null)
            {
                queryBuilder.Append(" USING");
                if (ttl != null)
                {
                    queryBuilder.Append(" TTL ?");
                    parameterList.Add(ttl.Value);
                    if (timestamp != null)
                    {
                        queryBuilder.Append(" AND");
                    }
                }
                if (timestamp != null)
                {
                    queryBuilder.Append(" TIMESTAMP ?");
                    parameterList.Add((timestamp.Value - UnixEpoch).Ticks / 10);
                }
            }
            queryParameters = parameterList.ToArray();
            return queryBuilder.ToString();
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
            return string.Format("UPDATE {0} SET {1} WHERE {2}", GetEscapedTableName(pocoData), nonPkColumns, pkColumns);
        }

        /// <summary>
        /// Prepends the CQL statement specified with "UPDATE tablename " for a POCO of Type T.
        /// </summary>
        public void PrependUpdate<T>(Cql cql)
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement(string.Format("UPDATE {0} {1}", GetEscapedTableName(pocoData), cql.Statement));
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
            return string.Format("DELETE FROM {0} WHERE {1}", GetEscapedTableName(pocoData), pkColumns);
        }

        /// <summary>
        /// Prepends the CQL statement specified with "DELETE FROM tablename " for a POCO of Type T.
        /// </summary>
        public void PrependDelete<T>(Cql cql)
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement(string.Format("DELETE FROM {0} {1}", GetEscapedTableName(pocoData), cql.Statement));
        }

        private static string GetTypeString(Serializer serializer, PocoColumn column)
        {
            string typeName;

            if (!column.IsCounter)
            {
                var typeCode = serializer.GetCqlType(column.ColumnType, out var typeInfo);
                typeName = GetTypeString(column, typeCode, typeInfo);
            }
            else
            {
                typeName = "counter";
            }

            if (column.IsStatic)
            {
                return typeName + " static";
            }

            return typeName;
        }

        /// <summary>
        /// Gets the CQL queries involved in a table creation (CREATE TABLE, CREATE INDEX)
        /// </summary>
        public static List<string> GetCreate(Serializer serializer, PocoData pocoData, string tableName, string keyspaceName, bool ifNotExists)
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
            var secondaryIndexes = new List<string>();
            var createTable = new StringBuilder("CREATE TABLE ");
            tableName = Escape(tableName, pocoData);
            if (keyspaceName != null)
            {
                //Use keyspace.tablename notation
                tableName = Escape(keyspaceName, pocoData) + "." + tableName;
            }
            createTable.Append(tableName);
            createTable.Append(" (");
            foreach (var column in pocoData.Columns)
            {
                var columnName = Escape(column.ColumnName, pocoData);
                createTable
                    .Append(columnName)
                    .Append(" ");
                var columnType = GetTypeString(serializer, column);
                createTable    
                    .Append(columnType);
                createTable
                    .Append(", ");
                if (column.SecondaryIndex)
                {
                    secondaryIndexes.Add(columnName);
                }
            }
            createTable.Append("PRIMARY KEY (");
            if (pocoData.PartitionKeys.Count == 0)
            {
                throw new InvalidOperationException("No partition key defined");
            }
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
            if (pocoData.CompactStorage)
            {
                createTable.Append(" WITH COMPACT STORAGE");
            }
            commands.Add(createTable.ToString());
            //Secondary index definitions
            commands.AddRange(secondaryIndexes.Select(name => "CREATE INDEX ON " + tableName + " (" + name + ")"));
            return commands;
        }

        private static string GetTypeString(PocoColumn column, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                //Is a single type
                return typeCode.ToString().ToLower();
            }
            string typeName = null;
            var frozenKey = column != null && column.HasFrozenKey;
            var frozenValue = column != null && column.HasFrozenValue;
            if (typeInfo is MapColumnInfo)
            {
                var mapInfo = (MapColumnInfo) typeInfo;
                typeName = "map<" +
                    WrapFrozen(frozenKey, GetTypeString(null, mapInfo.KeyTypeCode, mapInfo.KeyTypeInfo)) +
                    ", " +
                    WrapFrozen(frozenValue, GetTypeString(null, mapInfo.ValueTypeCode, mapInfo.ValueTypeInfo)) +
                    ">";
            }
            else if (typeInfo is SetColumnInfo)
            {
                var setInfo = (SetColumnInfo) typeInfo;
                typeName = "set<" +
                    WrapFrozen(frozenKey, GetTypeString(null, setInfo.KeyTypeCode, setInfo.KeyTypeInfo)) +
                    ">";
            }
            else if (typeInfo is ListColumnInfo)
            {
                var setInfo = (ListColumnInfo) typeInfo;
                typeName = "list<" +
                    WrapFrozen(frozenValue, GetTypeString(null, setInfo.ValueTypeCode, setInfo.ValueTypeInfo)) +
                    ">";
            }
            else if (typeInfo is TupleColumnInfo)
            {
                var tupleInfo = (TupleColumnInfo) typeInfo;
                typeName = "tuple<" +
                    string.Join(", ", tupleInfo.Elements.Select(e => GetTypeString(null, e.TypeCode, e.TypeInfo))) +
                    ">";
            }
            else if (typeInfo is UdtColumnInfo)
            {
                var udtInfo = (UdtColumnInfo) typeInfo;
                // Escape keyspace and name from the UDT
                typeName = string.Join(".", udtInfo.Name.Split('.').Select(k => "\"" + k + "\""));
            }

            if (typeName == null)
            {
                throw new NotSupportedException(string.Format("Type {0} is not supported", typeCode));
            }
            return WrapFrozen(column != null && column.IsFrozen, typeName);
        }

        private static string WrapFrozen(bool condition, string typeName)
        {
            if (condition)
            {
                return "frozen<" + typeName + ">";
            }
            return typeName;
        }
    }
}
