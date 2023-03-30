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
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra.Mapping
{
    /// <summary>
    /// A class for defining how to map a POCO via a fluent-style interface.  The mapping for Type T should be defined in the
    /// constructor of the sub class.
    /// </summary>
    public class Map<TPoco> : ITypeDefinition
    {
        private readonly Type _pocoType;
        private readonly Dictionary<string, ColumnMap> _columnMaps;

        private string _tableName;
        private bool _explicitColumns;
        private bool _caseSensitive;

        private string[] _partitionKeyColumns;
        private MemberInfo[] _partitionKeyColumnMembers;
        private readonly List<Tuple<string, SortOrder>> _clusteringKeyColumns = new List<Tuple<string, SortOrder>>(0);
        private readonly List<Tuple<MemberInfo, SortOrder>> _clusteringKeyColumnMembers = new List<Tuple<MemberInfo,SortOrder>>(0);
        private bool _compactStorage;
        private string _keyspaceName;

        Type ITypeDefinition.PocoType
        {
            get { return _pocoType; }
        }

        string ITypeDefinition.TableName
        {
            get { return _tableName; }
        }

        string ITypeDefinition.KeyspaceName
        {
            get { return _keyspaceName; }
        }

        bool ITypeDefinition.ExplicitColumns
        {
            get { return _explicitColumns; }
        }

        bool ITypeDefinition.CaseSensitive
        {
            get { return _caseSensitive; }
        }

        bool ITypeDefinition.AllowFiltering
        {
            get { return false; }
        }

        bool ITypeDefinition.CompactStorage
        {
            get { return _compactStorage; }
        }

        string[] ITypeDefinition.PartitionKeys
        {
            get
            {
                // Use string column names if configured
                if (_partitionKeyColumns != null)
                    return _partitionKeyColumns;

                // If no MemberInfos available either, just bail
                if (_partitionKeyColumnMembers == null) 
                    return null;

                // Get the column names from the members
                var columnNames = new string[_partitionKeyColumnMembers.Length];
                for (var index = 0; index < _partitionKeyColumnMembers.Length; index++)
                {
                    var memberInfo = _partitionKeyColumnMembers[index];
                    columnNames[index] = GetColumnName(memberInfo);
                }

                return columnNames;
            }
        }

        Tuple<string, SortOrder>[] ITypeDefinition.ClusteringKeys
        {
            get
            {
                //Need to concat the clustering keys by name
                //Plus the one defined by member
                return _clusteringKeyColumns
                    .Concat(_clusteringKeyColumnMembers.Select(i => Tuple.Create(GetColumnName(i.Item1), i.Item2)))
                    .ToArray();
            }
        }

        /// <summary>
        /// Creates a new fluent mapping definition for POCOs of Type TPoco.
        /// </summary>
        public Map()
        {
            _pocoType = typeof (TPoco);
            _columnMaps = new Dictionary<string, ColumnMap>();
        }

        /// <summary>
        /// Specifies what table to map the POCO to.
        /// </summary>
        public Map<TPoco> TableName(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            
            _tableName = tableName;
            return this;
        }

        /// <summary>
        /// Specifies the partition key column names for the table using the order provided.
        /// </summary>
        public Map<TPoco> PartitionKey(params string[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException("columnNames");
            if (columnNames.Length == 0) throw new ArgumentOutOfRangeException("columnNames", "Must specify at least one partition key column.");
            if (_partitionKeyColumnMembers != null) throw new InvalidOperationException("Partition key columns were already specified.");
            _partitionKeyColumns = columnNames;
            return this;
        }

        /// <summary>
        /// Specifies the properties/fields on the POCO whose column names are the partition key for the table.
        /// </summary>
        public Map<TPoco> PartitionKey(params Expression<Func<TPoco, object>>[] columns)
        {
            if (columns == null) throw new ArgumentNullException("columns");
            if (columns.Length == 0) throw new ArgumentOutOfRangeException("columns", "Must specify at least one partition key column.");
            if (_partitionKeyColumns != null) throw new InvalidOperationException("Partition key column names were already specified, define multiple using invoking this method with multiple expressions.");

            // Validate we got property/field expressions
            var partitionKeyMemberInfo = new MemberInfo[columns.Length];
            for (var index = 0; index < columns.Length; index++)
            {
                // If expression is good, add it to the array we're building (GetPropertyOrField should throw on invalid)
                var memberInfo = GetPropertyOrField(columns[index]);
                partitionKeyMemberInfo[index] = memberInfo;
            }

            // All expressions were good, so track accordingly
            _partitionKeyColumnMembers = partitionKeyMemberInfo;
            return this;
        }

        /// <summary>
        /// Specifies the clustering key column names for the table using the order provided.
        /// </summary>
        public Map<TPoco> ClusteringKey(params string[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException("columnNames");
            if (columnNames.Length == 0) return this;
            _clusteringKeyColumns.AddRange(columnNames.Select(name => Tuple.Create(name, SortOrder.Unspecified)));
            return this;
        }

        /// <summary>
        /// Specifies the Clustering keys with the corresponding clustering order
        /// </summary>
        public Map<TPoco> ClusteringKey(params Tuple<string, SortOrder>[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException("columnNames");
            if (columnNames.Length == 0) return this;
            //Allow multiple calls to clustering key
            _clusteringKeyColumns.AddRange(columnNames);
            return this;
        }

        /// <summary>
        /// Specifies a Clustering key with its clustering order
        /// </summary>
        /// <param name="column">Expression to select the property or the field</param>
        /// <param name="order">Clustering order</param>
        public Map<TPoco> ClusteringKey(Expression<Func<TPoco, object>> column, SortOrder order)
        {
            if (column == null) throw new ArgumentNullException("column");
            var memberInfo = GetPropertyOrField(column);
            _clusteringKeyColumnMembers.Add(Tuple.Create(memberInfo, order));
            return this;
        }

        /// <summary>
        /// Specifies a Clustering key with unspecified order
        /// </summary>
        /// <param name="column">Expression to select the property or the field</param>
        public Map<TPoco> ClusteringKey(Expression<Func<TPoco, object>> column)
        {
            return ClusteringKey(column, SortOrder.Unspecified);
        }

        /// <summary>
        /// Specifies that when mapping, we should only map columns that are explicitly defined here.  Use the Column method
        /// to define columns.
        /// </summary>
        public Map<TPoco> ExplicitColumns()
        {
            _explicitColumns = true;
            return this;
        }

        /// <summary>
        /// Specifies that when generating queries, the table and column names identifiers must be quoted. Defaults to false.
        /// </summary>
        /// <returns></returns>
        public Map<TPoco> CaseSensitive()
        {
            _caseSensitive = true;
            return this;
        }

        /// <summary>
        /// Defines options for mapping the column specified.
        /// </summary>
        public Map<TPoco> Column<TProp>(Expression<Func<TPoco, TProp>> column, Action<ColumnMap> columnConfig)
        {
            if (column == null) throw new ArgumentNullException("column");
            if (columnConfig == null) throw new ArgumentNullException("columnConfig");

            MemberInfo memberInfo = GetPropertyOrField(column);

            // Create the ColumnMap for the member if we haven't already
            if (_columnMaps.TryGetValue(memberInfo.Name, out ColumnMap columnMap) == false)
            {
                Type memberInfoType = memberInfo as PropertyInfo != null
                                          ? ((PropertyInfo)memberInfo).PropertyType
                                          : ((FieldInfo)memberInfo).FieldType;

                columnMap = new ColumnMap(memberInfo, memberInfoType, true);
                _columnMaps[memberInfo.Name] = columnMap;
            }

            // Run the configuration action on the column map
            columnConfig(columnMap);
            return this;
        }

        /// <summary>
        /// Specifies that when mapping, the table name should include the keyspace.
        /// Use only if the table you are mapping is in a different keyspace than the current <see cref="ISession"/>.
        /// </summary>
        public Map<TPoco> KeyspaceName(string name)
        {
            _keyspaceName = name;
            return this;
        }

        /// <summary>
        /// Specifies that the table is defined as COMPACT STORAGE
        /// </summary>
        /// <returns></returns>
        public Map<TPoco> CompactStorage()
        {
            _compactStorage = true;
            return this;
        }

        /// <summary>
        /// Sets the mapping for the expression using the default options.
        /// </summary>
        public Map<TPoco> Column<TProp>(Expression<Func<TPoco, TProp>> column)
        {
            return Column(column, _ => { });
        }

        IColumnDefinition ITypeDefinition.GetColumnDefinition(FieldInfo field)
        {
            // If a column map has been defined, return it, otherwise create an empty one
            return _columnMaps.TryGetValue(field.Name, out ColumnMap columnMap) ? columnMap : new ColumnMap(field, field.FieldType, false);
        }

        IColumnDefinition ITypeDefinition.GetColumnDefinition(PropertyInfo property)
        {
            // If a column map has been defined, return it, otherwise create an empty one
            return _columnMaps.TryGetValue(property.Name, out ColumnMap columnMap) ? columnMap : new ColumnMap(property, property.PropertyType, false);
        }

        private string GetColumnName(MemberInfo memberInfo)
        {
            if (_columnMaps.TryGetValue(memberInfo.Name, out ColumnMap columnMap))
            {
                return ((IColumnDefinition)columnMap).ColumnName ?? memberInfo.Name;
            }
            return memberInfo.Name;
        }

        /// <summary>
        /// Gets the MemberInfo for the property or field that the expression provided refers to.  Will throw if the Expression does not refer
        /// to a valid property or field on TPoco.
        /// </summary>
        private MemberInfo GetPropertyOrField<TProp>(Expression<Func<TPoco, TProp>> expression)
        {
            // Take the body of the lambda expression
            Expression body = expression.Body;

            // We'll get a Convert node for the Func<TPoco, object> where the actual property expression is the operand being converted to object
            if (body.NodeType == ExpressionType.Convert)
                body = ((UnaryExpression) body).Operand;

            var memberExpression = body as MemberExpression;
            if (memberExpression == null || IsPropertyOrField(memberExpression.Member) == false)
                throw new ArgumentOutOfRangeException("expression", string.Format("Expression {0} is not a property or field.", expression));

            if (memberExpression.Member.DeclaringType != _pocoType && _pocoType.GetTypeInfo().IsSubclassOf(memberExpression.Member.DeclaringType) == false)
            {
                throw new ArgumentOutOfRangeException("expression",
                                                      string.Format("Expression {0} refers to a property or field that is not from type {1}",
                                                                    expression, _pocoType));
            }
                
            return memberExpression.Member;
        }

        private static bool IsPropertyOrField(MemberInfo memberInfo)
        {
            return memberInfo.MemberType == MemberTypes.Field || memberInfo.MemberType == MemberTypes.Property;
        }
    }
}
