//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

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
        private const string NoColumns = CqlGenerator.CannotGenerateStatementForPoco + " because it has no columns";

        private const string MissingPkColumns = CqlGenerator.CannotGenerateStatementForPoco + " because it is missing PK columns {2}.  " +
                                                "Are you missing a property/field on the POCO or did you forget to specify " +
                                                "the PK columns in the mapping?";

        private static readonly Regex SelectRegex = new Regex(@"\A\s*SELECT\s", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex FromRegex = new Regex(@"\A\s*FROM\s", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly PocoDataFactory _pocoDataFactory;
        private static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        private static readonly ICqlIdentifierHelper CqlIdentifierHelper = new CqlIdentifierHelper();

        public CqlGenerator(PocoDataFactory pocoDataFactory)
        {
            _pocoDataFactory = pocoDataFactory ?? throw new ArgumentNullException(nameof(pocoDataFactory));
        }

        /// <summary>
        /// Adds "SELECT columnlist" and "FROM tablename" to a CQL statement if they don't already exist for a POCO of Type T.
        /// </summary>
        public void AddSelect<T>(Cql cql)
        {
            // If it's already got a SELECT clause, just bail
            if (CqlGenerator.SelectRegex.IsMatch(cql.Statement))
                return;

            // Get the PocoData so we can generate a list of columns
            var pocoData = _pocoDataFactory.GetPocoData<T>();
            var allColumns = pocoData.Columns.Select(CqlGenerator.EscapeFunc(pocoData)).ToCommaDelimitedString();

            var suffix = string.IsNullOrEmpty(cql.Statement) ? string.Empty : " " + cql.Statement;

            // If it's got the from clause, leave FROM intact, otherwise add it
            cql.SetStatement(CqlGenerator.FromRegex.IsMatch(cql.Statement)
                                 ? $"SELECT {allColumns}{suffix}"
                                 : $"SELECT {allColumns} FROM " +
                                   $"{CqlGenerator.CqlIdentifierHelper.EscapeTableNameIfNecessary(pocoData, pocoData.KeyspaceName, pocoData.TableName)}" +
                                   $"{suffix}");
        }
        
        private static Func<PocoColumn, string> EscapeFunc(PocoData pocoData)
        {
            Func<PocoColumn, string> f = 
                c => CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, c.ColumnName);
            return f;
        }

        private static Func<PocoColumn, string> EscapeFunc(PocoData pocoData, string format)
        {
            Func<PocoColumn, string> f = 
                c => string.Format(format, CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, c.ColumnName));
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
                throw new InvalidOperationException(string.Format(CqlGenerator.NoColumns, "INSERT", typeof(T).Name));
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
                    columnsBuilder.Append(CqlGenerator.EscapeFunc(pocoData)(pocoData.Columns[i]));
                    placeholdersBuilder.Append("?");
                    parameterList.Add(value);
                }
                columns = columnsBuilder.ToString();
                placeholders = placeholdersBuilder.ToString();
            }
            else
            {
                //Include all columns defined in the Poco
                columns = pocoData.Columns.Select(CqlGenerator.EscapeFunc(pocoData)).ToCommaDelimitedString();
                placeholders = Enumerable.Repeat("?", pocoData.Columns.Count).ToCommaDelimitedString();
                parameterList.AddRange(pocoValues);
            }
            var queryBuilder = new StringBuilder();
            queryBuilder.Append("INSERT INTO ");
            queryBuilder.Append(tableName ?? CqlGenerator.CqlIdentifierHelper.EscapeTableNameIfNecessary(pocoData, pocoData.KeyspaceName, pocoData.TableName));
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
                    parameterList.Add((timestamp.Value - CqlGenerator.UnixEpoch).Ticks / 10);
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
                throw new InvalidOperationException(string.Format(CqlGenerator.NoColumns, "UPDATE", typeof(T).Name));

            if (pocoData.MissingPrimaryKeyColumns.Count > 0)
            {
                throw new InvalidOperationException(string.Format(CqlGenerator.MissingPkColumns, "UPDATE", typeof(T).Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }

            var nonPkColumns = pocoData.GetNonPrimaryKeyColumns().Select(CqlGenerator.EscapeFunc(pocoData, "{0} = ?")).ToCommaDelimitedString();
            var pkColumns = string.Join(" AND ", pocoData.GetPrimaryKeyColumns().Select(CqlGenerator.EscapeFunc(pocoData, "{0} = ?")));
            return $"UPDATE {CqlGenerator.CqlIdentifierHelper.EscapeTableNameIfNecessary(pocoData, pocoData.KeyspaceName, pocoData.TableName)} SET {nonPkColumns} WHERE {pkColumns}";
        }

        /// <summary>
        /// Prepends the CQL statement specified with "UPDATE tablename " for a POCO of Type T.
        /// </summary>
        public void PrependUpdate<T>(Cql cql)
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement($"UPDATE {CqlGenerator.CqlIdentifierHelper.EscapeTableNameIfNecessary(pocoData, pocoData.KeyspaceName, pocoData.TableName)} {cql.Statement}");
        }

        /// <summary>
        /// Generates a "DELETE FROM tablename WHERE pkcolumns = ?" statement for a POCO of Type T.
        /// </summary>
        public string GenerateDelete<T>()
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();

            if (pocoData.Columns.Count == 0)
            {
                throw new InvalidOperationException(string.Format(CqlGenerator.NoColumns, "DELETE", typeof(T).Name));
            }

            if (pocoData.MissingPrimaryKeyColumns.Count > 0)
            {
                throw new InvalidOperationException(string.Format(CqlGenerator.MissingPkColumns, "DELETE", typeof(T).Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }

            var pkColumns = string.Join(" AND ", pocoData.GetPrimaryKeyColumns().Select(CqlGenerator.EscapeFunc(pocoData, "{0} = ?")));
            return $"DELETE FROM {CqlGenerator.CqlIdentifierHelper.EscapeTableNameIfNecessary(pocoData, pocoData.KeyspaceName, pocoData.TableName)} WHERE {pkColumns}";
        }

        /// <summary>
        /// Prepends the CQL statement specified with "DELETE FROM tablename " for a POCO of Type T.
        /// </summary>
        public void PrependDelete<T>(Cql cql)
        {
            var pocoData = _pocoDataFactory.GetPocoData<T>();
            cql.SetStatement($"DELETE FROM {CqlGenerator.CqlIdentifierHelper.EscapeTableNameIfNecessary(pocoData, pocoData.KeyspaceName, pocoData.TableName)} {cql.Statement}");
        }

        private static string GetTypeString(ISerializer serializer, PocoColumn column)
        {
            string typeName;

            if (!column.IsCounter)
            {
                var typeCode = serializer.GetCqlType(column.ColumnType, out var typeInfo);
                typeName = CqlGenerator.GetTypeString(column, typeCode, typeInfo);
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
        public static List<string> GetCreate(ISerializer serializer, PocoData pocoData, string tableName, string keyspaceName, bool ifNotExists)
        {
            if (pocoData == null)
            {
                throw new ArgumentNullException(nameof(pocoData));
            }
            if (pocoData.MissingPrimaryKeyColumns.Count > 0)
            {
                throw new InvalidOperationException(string.Format(CqlGenerator.MissingPkColumns, "CREATE", pocoData.PocoType.Name,
                                                                  pocoData.MissingPrimaryKeyColumns.ToCommaDelimitedString()));
            }
            var commands = new List<string>();
            var secondaryIndexes = new List<string>();
            var createTable = new StringBuilder("CREATE TABLE ");
            tableName = CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, tableName);
            if (keyspaceName != null)
            {
                //Use keyspace.tablename notation
                tableName = CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, keyspaceName) + "." + tableName;
            }
            createTable.Append(tableName);
            createTable.Append(" (");
            foreach (var column in pocoData.Columns)
            {
                var columnName = CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, column.ColumnName);
                createTable
                    .Append(columnName)
                    .Append(" ");
                var columnType = CqlGenerator.GetTypeString(serializer, column);
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
                createTable.Append(CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, pocoData.PartitionKeys[0].ColumnName));
            }
            else
            {
                //tupled partition keys
                createTable
                    .Append("(")
                    .Append(string.Join(", ", pocoData.PartitionKeys.Select(CqlGenerator.EscapeFunc(pocoData))))
                    .Append(")");
            }
            if (pocoData.ClusteringKeys.Count > 0)
            {
                createTable.Append(", ");
                createTable.Append(string.Join(
                    ", ", 
                    pocoData.ClusteringKeys.Select(
                        k => CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, k.Item1.ColumnName))));
            }
            //close primary keys
            createTable.Append(")");
            //close table column definition
            createTable.Append(")");
            var clusteringOrder = string.Join(", ", pocoData.ClusteringKeys
                .Where(k => k.Item2 != SortOrder.Unspecified)
                .Select(k => 
                    CqlGenerator.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, k.Item1.ColumnName) 
                    + " " 
                    + (k.Item2 == SortOrder.Ascending ? "ASC" : "DESC")));

            var clusteringOrderIsDefined = !string.IsNullOrEmpty(clusteringOrder);
            if (clusteringOrderIsDefined)
            {
                createTable
                    .Append(" WITH CLUSTERING ORDER BY (")
                    .Append(clusteringOrder)
                    .Append(")");
            }
            if (pocoData.CompactStorage)
            {
                createTable.Append($" {(clusteringOrderIsDefined ? "AND" : "WITH")} COMPACT STORAGE");
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
            if (typeInfo is MapColumnInfo mapInfo)
            {
                typeName = "map<" +
                    CqlGenerator.WrapFrozen(frozenKey, CqlGenerator.GetTypeString(null, mapInfo.KeyTypeCode, mapInfo.KeyTypeInfo)) +
                    ", " +
                    CqlGenerator.WrapFrozen(frozenValue, CqlGenerator.GetTypeString(null, mapInfo.ValueTypeCode, mapInfo.ValueTypeInfo)) +
                    ">";
            }
            else if (typeInfo is SetColumnInfo setInfo1)
            {
                typeName = "set<" +
                    CqlGenerator.WrapFrozen(frozenKey, CqlGenerator.GetTypeString(null, setInfo1.KeyTypeCode, setInfo1.KeyTypeInfo)) +
                    ">";
            }
            else if (typeInfo is ListColumnInfo setInfo)
            {
                typeName = "list<" +
                    CqlGenerator.WrapFrozen(frozenValue, CqlGenerator.GetTypeString(null, setInfo.ValueTypeCode, setInfo.ValueTypeInfo)) +
                    ">";
            }
            else if (typeInfo is TupleColumnInfo tupleInfo)
            {
                typeName = "tuple<" +
                    string.Join(", ", tupleInfo.Elements.Select(e => CqlGenerator.GetTypeString(null, e.TypeCode, e.TypeInfo))) +
                    ">";
            }
            else if (typeInfo is UdtColumnInfo udtInfo)
            {
                // Escape keyspace and name from the UDT
                typeName = string.Join(".", udtInfo.Name.Split('.').Select(k => "\"" + k + "\""));
            }

            if (typeName == null)
            {
                throw new NotSupportedException($"Type {typeCode} is not supported");
            }
            return CqlGenerator.WrapFrozen(column != null && column.IsFrozen, typeName);
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
