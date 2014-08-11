using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using CqlPoco.Mapping;

namespace CqlPoco.FluentMapping
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

        private string[] _primaryKeyColumns;
        private MemberInfo[] _primaryKeyColumnMembers;
        
        Type ITypeDefinition.PocoType
        {
            get { return _pocoType; }
        }

        string ITypeDefinition.TableName
        {
            get { return _tableName; }
        }

        bool ITypeDefinition.ExplicitColumns
        {
            get { return _explicitColumns; }
        }

        string[] ITypeDefinition.PrimaryKeyColumns
        {
            get
            {
                // Use string column names if configured
                if (_primaryKeyColumns != null)
                    return _primaryKeyColumns;

                // If no MemberInfos available either, just bail
                if (_primaryKeyColumnMembers == null) 
                    return null;

                // Get the column names from the members
                var columnNames = new string[_primaryKeyColumnMembers.Length];
                for (int index = 0; index < _primaryKeyColumnMembers.Length; index++)
                {
                    MemberInfo memberInfo = _primaryKeyColumnMembers[index];

                    // Try to get a column definition for each of the columns and if we can't find one or the column name is not defined,
                    // just default to the field/property name
                    ColumnMap columnMap;
                    if (_columnMaps.TryGetValue(memberInfo.Name, out columnMap))
                        columnNames[index] = ((IColumnDefinition) columnMap).ColumnName ?? memberInfo.Name;

                    columnNames[index] = memberInfo.Name;
                }

                return columnNames;
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
        /// Specifies the primary key column names for the table.
        /// </summary>
        public Map<TPoco> PrimaryKey(params string[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException("columnNames");
            if (columnNames.Length == 0) throw new ArgumentOutOfRangeException("columnNames", "Must specify at least one primary key column.");
            if (_primaryKeyColumnMembers != null) throw new InvalidOperationException("Primary key columns were already specified.");

            _primaryKeyColumns = columnNames;
            return this;
        }

        /// <summary>
        /// Specifies the properties/fields on the POCO whose column names are the primary key for the table.
        /// </summary>
        public Map<TPoco> PrimaryKey(params Expression<Func<TPoco, object>>[] columns)
        {
            if (columns == null) throw new ArgumentNullException("columns");
            if (columns.Length == 0) throw new ArgumentOutOfRangeException("columns", "Must specify at least one primary key column.");
            if (_primaryKeyColumns != null) throw new InvalidOperationException("Primary key column names were already specified.");

            // Validate we got property/field expressions
            var primaryKeyMemberInfo = new MemberInfo[columns.Length];
            for (int index = 0; index < columns.Length; index++)
            {
                // If expression is good, add it to the array we're building (GetPropertyOrField should throw on invalid)
                MemberInfo memberInfo = GetPropertyOrField(columns[index]);
                primaryKeyMemberInfo[index] = memberInfo;
            }

            // All expressions were good, so track accordingly
            _primaryKeyColumnMembers = primaryKeyMemberInfo;
            return this;
        }

        /// <summary>
        /// Specifies that when mapping, we should only map columns that are explicitly defined here.  Use the <see cref="Column{TProp}"/> method
        /// to define columns.
        /// </summary>
        public Map<TPoco> ExplicitColumns()
        {
            _explicitColumns = true;
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
            ColumnMap columnMap;
            if (_columnMaps.TryGetValue(memberInfo.Name, out columnMap) == false)
            {
                Type memberInfoType = memberInfo as PropertyInfo != null
                                          ? ((PropertyInfo) memberInfo).PropertyType
                                          : ((FieldInfo) memberInfo).FieldType;

                columnMap = new ColumnMap(memberInfo, memberInfoType, true);
                _columnMaps[memberInfo.Name] = columnMap;
            }

            // Run the configuration action on the column map
            columnConfig(columnMap);
            return this;
        }
            
        IColumnDefinition ITypeDefinition.GetColumnDefinition(FieldInfo field)
        {
            // If a column map has been defined, return it, otherwise create an empty one
            ColumnMap columnMap;
            return _columnMaps.TryGetValue(field.Name, out columnMap) ? columnMap : new ColumnMap(field, field.FieldType, false);
        }

        IColumnDefinition ITypeDefinition.GetColumnDefinition(PropertyInfo property)
        {
            // If a column map has been defined, return it, otherwise create an empty one
            ColumnMap columnMap;
            return _columnMaps.TryGetValue(property.Name, out columnMap) ? columnMap : new ColumnMap(property, property.PropertyType, false);
        }

        /// <summary>
        /// Gets the MemberInfo for the property or field that the expression provided refers to.  Will throw if the Expression does not refer
        /// to a valid property or field on TPoco.
        /// </summary>
        private MemberInfo GetPropertyOrField<TProp>(Expression<Func<TPoco, TProp>> expression)
        {
            // Take the body of the lamdba expression
            Expression body = expression.Body;

            // We'll get a Convert node for the Func<TPoco, object> where the actual property expression is the operand being converted to object
            if (body.NodeType == ExpressionType.Convert)
                body = ((UnaryExpression) body).Operand;

            var memberExpression = body as MemberExpression;
            if (memberExpression == null || IsPropertyOrField(memberExpression.Member) == false)
                throw new ArgumentOutOfRangeException("expression", string.Format("Expression {0} is not a property or field.", expression));

            if (memberExpression.Member.ReflectedType != _pocoType && _pocoType.IsSubclassOf(memberExpression.Member.ReflectedType) == false)
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
