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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra.Mapping.TypeConversion;

namespace Cassandra.Mapping
{
    /// <summary>
    /// Component capable of getting/creating Func&lt;Row, T&gt; functions that create POCOs of type T from a Cassandra Row.
    /// </summary>
    internal class MapperFactory
    {
        private static readonly Type CassandraRowType = typeof (Row);
        private static readonly Type IntType = typeof (int);
        private static readonly Type ObjectType = typeof (object);

        private const BindingFlags PublicInstance = BindingFlags.Public | BindingFlags.Instance;

        // The Row.GetValue<T>(int column) instance method
        private static readonly MethodInfo GetValueOfTMethod = CassandraRowType.GetTypeInfo().GetMethods(PublicInstance).Single(mi =>
        {
            if (mi.Name != "GetValue" || mi.IsGenericMethodDefinition == false)
                return false;

            Type[] genericArgs = mi.GetGenericArguments();
            if (genericArgs.Length != 1)
                return false;

            ParameterInfo[] parameters = mi.GetParameters();
            return parameters.Length == 1 && parameters[0].ParameterType == IntType;
        });

        // The Row.IsNull(int column) instance method
        private static readonly MethodInfo IsNullMethod = CassandraRowType.GetTypeInfo().GetMethods(PublicInstance).Single(mi =>
        {
            if (mi.Name != "IsNull")
                return false;

            ParameterInfo[] parameters = mi.GetParameters();
            return parameters.Length == 1 && parameters[0].ParameterType == IntType;
        });

        private readonly TypeConverter _typeConverter;
        private readonly PocoDataFactory _pocoDataFactory;
        private readonly ConcurrentDictionary<Tuple<Type, string, string>, Delegate> _mapperFuncCache;
        private readonly ConcurrentDictionary<Tuple<Type, string>, Delegate> _valueCollectorFuncCache;

        public TypeConverter TypeConverter
        {
            get { return _typeConverter; }
        }

        public PocoDataFactory PocoDataFactory
        {
            get { return _pocoDataFactory; }
        }

        public MapperFactory(TypeConverter typeConverter, PocoDataFactory pocoDataFactory)
        {
            _typeConverter = typeConverter ?? throw new ArgumentNullException("typeConverter");
            _pocoDataFactory = pocoDataFactory ?? throw new ArgumentNullException("pocoDataFactory");

            _mapperFuncCache = new ConcurrentDictionary<Tuple<Type, string, string>, Delegate>();
            _valueCollectorFuncCache = new ConcurrentDictionary<Tuple<Type, string>, Delegate>();
        }

        /// <summary>
        /// Gets a mapper Func that can map from a C* row to the POCO type T for the given statement.
        /// </summary>
        public Func<Row, T> GetMapper<T>(string cql, RowSet rows)
        {
            var key = GetMapperCacheKey<T>(cql);
            var mapperFunc = _mapperFuncCache.GetOrAdd(key, _ => CreateMapper<T>(rows));
            return (Func<Row, T>)mapperFunc;
        }

        public Func<Row, T> GetMapperWithProjection<T>(string cql, RowSet rows, Expression projectionExpression)
        {
            // Use ExpressionStringBuilder to build the string representation, which should not be drag for
            // small projections
            var key = GetMapperCacheKey<T>(cql, projectionExpression.ToString());
            var mapperFunc = _mapperFuncCache
                .GetOrAdd(key, _ => CreateMapperWithProjection<T>(rows, projectionExpression));
            return (Func<Row, T>)mapperFunc;
        }

        private static Tuple<Type, string, string> GetMapperCacheKey<T>(string cql, string additional = null)
        {
            return Tuple.Create(typeof(T), cql, additional ?? "");
        }

        /// <summary>
        /// Gets a Func that can collect all the values on a given POCO T and return an object[] of those values in the same
        /// order as the PocoColumns for T's PocoData.
        /// </summary>
        public Func<T, object[]> GetValueCollector<T>(string cql, bool primaryKeyValuesOnly = false, bool primaryKeyValuesLast = false)
        {
            Tuple<Type, string> key = Tuple.Create(typeof (T), cql);
            Delegate valueCollectorFunc = _valueCollectorFuncCache.GetOrAdd(key, _ => CreateValueCollector<T>(primaryKeyValuesOnly, primaryKeyValuesLast));
            return (Func<T, object[]>) valueCollectorFunc;
        }
        
        /// <summary>
        /// Creates a mapper Func for going from a C* Row to a POCO, T.
        /// </summary>
        private Func<Row, T> CreateMapper<T>(RowSet rows)
        {
            var pocoData = GetPocoData<T>();

            // See if we retrieved only one column and if that column does not exist in the PocoData
            if (rows.Columns.Length == 1 && 
                !Cassandra.Utils.IsAnonymousType(pocoData.PocoType) && 
                !pocoData.Columns.Contains(rows.Columns[0].Name))
            {
                // Map the single column value directly to the POCO
                return CreateMapperForSingleColumnToPoco<T>(rows, pocoData);
            }
            
            // Create a default POCO mapper
            return CreateMapperForPoco<T>(rows, pocoData);
        }

        private Func<Row, T> CreateMapperWithProjection<T>(RowSet rows, Expression projectionExpression)
        {
            var pocoData = GetPocoData<T>();

            // See if we retrieved only one column and if that column does not exist in the PocoData
            if (rows.Columns.Length == 1 && typeof(T).GetTypeInfo().IsAssignableFrom(rows.Columns[0].Type))
            {
                // Map the single column value directly to the POCO
                return CreateMapperForSingleColumnToPoco<T>(rows, pocoData);
            }
            var expressionVisitor = new ProjectionExpressionVisitor();
            expressionVisitor.Visit(projectionExpression);
            // Process the expression to extract the constructor and properties involved
            var projection = expressionVisitor.Projection;
            if (projection == null)
            {
                throw new NotSupportedException("Projection expression not supported: " + projectionExpression);
            }
            // Create a mapper for the given projection
            return CreateMapperForProjection<T>(rows, projection);
        }

        public PocoData GetPocoData<T>()
        {
            return _pocoDataFactory.GetPocoData<T>();
        }

        /// <summary>
        /// Creates a Func that collects all the values from a POCO (of type T) into an object[], with the values being in the array in the
        /// same order as the POCO's PocoData.Columns collection.
        /// </summary>
        /// <param name="primaryKeyValuesOnly">Determines if only the primary key values should be extracted</param>
        /// <param name="primaryKeyValuesLast">Determines if only the values should contain first the non primary keys and then the primary keys</param>
        private Func<T, object[]> CreateValueCollector<T>(bool primaryKeyValuesOnly, bool primaryKeyValuesLast)
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();

            // Create list to hold the method body and our input parameter, the POCO of type T
            var methodBodyExpressions = new List<Expression>();
            ParameterExpression poco = Expression.Parameter(pocoData.PocoType, "poco");

            // Figure out which collection of columns to use
            IList<PocoColumn> columns = pocoData.Columns;
            if (primaryKeyValuesOnly)
            {
                //pk columns only
                columns = pocoData.GetPrimaryKeyColumns();
            }
            if (primaryKeyValuesLast)
            {
                //normal columns + pk columns
                var pkColumns = pocoData.GetPrimaryKeyColumns();
                columns = pocoData.Columns
                    .Except(pkColumns)
                    .Concat(pkColumns)
                    .ToList();
            }

            // Create a variable to hold our return value, and initialize as an object[] of correct size
            var values = Expression.Variable(typeof (object[]), "values");
            methodBodyExpressions.Add(
                // object[] values = new object[... number of columns on POCO ...];
                Expression.Assign(values, Expression.NewArrayBounds(ObjectType, Expression.Constant(columns.Count, IntType))));
            
            for (var idx = 0; idx < columns.Count; idx++)
            {
                PocoColumn column = columns[idx];

                Expression getValueFromPoco = GetExpressionToGetValueFromPoco(poco, column);
                
                // values[columnIndex] = (object) ... getValueFromPoco ...
                methodBodyExpressions.Add(
                    Expression.Assign(Expression.ArrayAccess(values, Expression.Constant(idx, IntType)),
                                      Expression.Convert(getValueFromPoco, ObjectType))
                    );
            }

            // Return our values array
            methodBodyExpressions.Add(values);

            // Construct the method body, then create a compiled Func for the method
            var methodBody = Expression.Block(new[] {values}, methodBodyExpressions);
            return Expression.Lambda<Func<T, object[]>>(methodBody, poco).Compile();
        }

        /// <summary>
        /// Creates a mapper that flattens a single column's value directly to the "POCO"'s value.  (POCO here most likely being a value type/string/etc.)
        /// </summary>
        private Func<Row, T> CreateMapperForSingleColumnToPoco<T>(RowSet rows, PocoData pocoData)
        {
            ParameterExpression row = Expression.Parameter(CassandraRowType, "row");

            CqlColumn dbColumn = rows.Columns[0];

            // Get an expression for getting the value of the single column as TPoco (and returning it)
            Expression getColumnOrDefault = GetExpressionToGetColumnValueFromRow(row, dbColumn, pocoData.PocoType);

            return Expression.Lambda<Func<Row, T>>(getColumnOrDefault, row).Compile();
        }

        /// <summary>
        /// Creates a mapper Func for a POCO.
        /// </summary>
        private Func<Row, T> CreateMapperForPoco<T>(RowSet rows, PocoData pocoData)
        {
            // We're going to store the method body expressions in a list since we need to use some looping to generate it
            ICollection<Expression> methodBodyExpressions = new LinkedList<Expression>();

            // The input parameter for our Func<Row, T>, a C* Row
            ParameterExpression row = Expression.Parameter(CassandraRowType, "row");

            // T poco = new T();
            var poco = Expression.Variable(pocoData.PocoType, "poco");
            if (pocoData.PocoType.GetTypeInfo().GetConstructor(Type.EmptyTypes) != null)
            {
                //It has default constructor
                methodBodyExpressions.Add(Expression.Assign(poco, Expression.New(pocoData.PocoType)));
            }
            else
            {
                var constructor = pocoData.PocoType.GetTypeInfo().GetConstructors().FirstOrDefault(c => c.GetParameters().Length == rows.Columns.Length);
                if (constructor == null)
                {
                    throw new ArgumentException(
                        string.Format("RowSet columns length is {0} but type {1} does not contain a constructor with the same amount of parameters", 
                        rows.Columns.Length,
                        pocoData.PocoType));
                }
                var parameterInfos = constructor.GetParameters();
                var parameterExpressions = new List<Expression>();
                for (var i = 0; i < rows.Columns.Length; i++)
                {
                    var c = rows.Columns[i];
                    var param = parameterInfos[i];
                    var getValueT = GetExpressionToGetColumnValueFromRow(row, c, param.ParameterType);
                    parameterExpressions.Add(getValueT);
                }
                methodBodyExpressions.Add(Expression.Assign(poco, Expression.New(constructor, parameterExpressions)));
            }

            // Keep track of any variables we need in the method body, starting with the poco variable
            var methodBodyVariables = new List<ParameterExpression> { poco };

            foreach (var dbColumn in rows.Columns)
            {
                // Try to find a corresponding column on the POCO and if not found, don't map that column from the RowSet
                if (pocoData.Columns.TryGetItem(dbColumn.Name, out PocoColumn pocoColumn) == false)
                    continue;

                // Figure out if we're going to need to do any casting/conversion when we call Row.GetValue<T>(columnIndex)
                Expression getColumnValue = GetExpressionToGetColumnValueFromRow(row, dbColumn, pocoColumn.MemberInfoType);

                // poco.SomeFieldOrProp = ... getColumnValue call ...
                BinaryExpression getValueAndAssign = Expression.Assign(Expression.MakeMemberAccess(poco, pocoColumn.MemberInfo), getColumnValue);

                // Start with an expression that does nothing if the row is null
                Expression ifRowValueIsNull = Expression.Empty();

                // Cassandra will return null for empty collections, so make an effort to populate collection properties on the POCO with
                // empty collections instead of null in those cases
                if (TryGetCreateEmptyCollectionExpression(dbColumn, pocoColumn.MemberInfoType, out Expression createEmptyCollection))
                {
                    // poco.SomeFieldOrProp = ... createEmptyCollection ...
                    ifRowValueIsNull = Expression.Assign(Expression.MakeMemberAccess(poco, pocoColumn.MemberInfo), createEmptyCollection);
                }

                var columnIndex = Expression.Constant(dbColumn.Index, IntType);
                //Expression equivalent to
                // if (row.IsNull(columnIndex) == false) => getValueAndAssign ...
                // else => ifRowIsNull ...
                methodBodyExpressions.Add(Expression.IfThenElse(Expression.IsFalse(Expression.Call(row, IsNullMethod, columnIndex)),
                    getValueAndAssign,
                    ifRowValueIsNull));
            }

            // The last expression in the method body is the return value, so put our new POCO at the end
            methodBodyExpressions.Add(poco);

            // Create a block expression for the method body expressions
            BlockExpression methodBody = Expression.Block(methodBodyVariables, methodBodyExpressions);

            // Return compiled expression
            return Expression.Lambda<Func<Row, T>>(methodBody, row).Compile();
        }

        /// <summary>
        /// Creates a mapper Func for a projection.
        /// </summary>
        private Func<Row, T> CreateMapperForProjection<T>(RowSet rows, NewTypeProjection projection)
        {
            ICollection<Expression> methodBodyExpressions = new LinkedList<Expression>();
            // The input parameter for our Func<Row, T>, a C* Row
            ParameterExpression row = Expression.Parameter(CassandraRowType, "row");
            // poco variable
            ParameterExpression poco = Expression.Variable(typeof(T), "poco");
            var constructorParameters = projection.ConstructorInfo.GetParameters();
            var columnIndex = 0;
            if (constructorParameters.Length == 0)
            {
                // Use default constructor
                // T poco = new T()
                methodBodyExpressions.Add(Expression.Assign(poco, Expression.New(projection.ConstructorInfo)));
            }
            else
            {
                var parameterExpressions = new List<Expression>();
                if (constructorParameters.Length > rows.Columns.Length)
                {
                    throw new IndexOutOfRangeException(string.Format("Expected at least {0} column(s), obtained {1}",
                        constructorParameters.Length, rows.Columns.Length));
                }
                for (columnIndex = 0; columnIndex < constructorParameters.Length; columnIndex++)
                {
                    var c = rows.Columns[columnIndex];
                    var param = constructorParameters[columnIndex];
                    var getValueT = GetExpressionToGetColumnValueFromRow(row, c, param.ParameterType);
                    parameterExpressions.Add(getValueT);
                }
                // T poco = new T(param1, param2, ...)
                methodBodyExpressions.Add(Expression.Assign(poco, 
                    Expression.New(projection.ConstructorInfo, parameterExpressions)));
            }
            // Fill the rest of members with the rest of the column values
            // ReSharper disable once GenericEnumeratorNotDisposed
            var membersEnumerator = projection.Members.GetEnumerator();
            while (membersEnumerator.MoveNext() && columnIndex < rows.Columns.Length)
            {
                var member = membersEnumerator.Current;
                var c = rows.Columns[columnIndex];
                var memberType = GetUnderlyingType(member);
                var getColumnValue = GetExpressionToGetColumnValueFromRow(row, c, memberType);
                // poco.SomeFieldOrProp = ... getColumnValue call ...
                var getValueAndAssign = Expression.Assign(
                    Expression.MakeMemberAccess(poco, member), getColumnValue);
                // Start with an expression that does nothing if the row is null
                Expression ifRowValueIsNull = Expression.Empty();
                // For collections, make an effort to return an empty collection instead of null
                if (TryGetCreateEmptyCollectionExpression(c, memberType, out Expression createEmptyCollection))
                {
                    // poco.SomeFieldOrProp = ... createEmptyCollection ...
                    ifRowValueIsNull = Expression.Assign(Expression.MakeMemberAccess(poco, member), createEmptyCollection);
                }

                var columnIndexExpression = Expression.Constant(columnIndex, IntType);
                //Expression equivalent to
                // if (row.IsNull(columnIndex) == false) => getValueAndAssign ...
                // else => ifRowIsNull ...
                methodBodyExpressions.Add(Expression.IfThenElse(
                    Expression.IsFalse(Expression.Call(row, IsNullMethod, columnIndexExpression)), 
                    getValueAndAssign, 
                    ifRowValueIsNull));
                columnIndex++;
            }
            // The last expression in the method body is the return value, so put our new POCO at the end
            methodBodyExpressions.Add(poco);
            // Create a block expression for the method body expressions
            var methodBody = Expression.Block(new [] { poco }, methodBodyExpressions);
            // Return compiled expression
            return Expression.Lambda<Func<Row, T>>(methodBody, row).Compile();
        }

        private static Type GetUnderlyingType(MemberInfo member)
        {
            switch (member.MemberType)
            {
                case MemberTypes.Field:
                    return ((FieldInfo)member).FieldType;
                case MemberTypes.Property:
                    return ((PropertyInfo)member).PropertyType;
                default:
                    throw new NotSupportedException("Only FieldInfo and PropertyInfo are supported");
            }
        }

        /// <summary>
            /// Gets an Expression that gets the value of a POCO field or property.
            /// </summary>
        private Expression GetExpressionToGetValueFromPoco(ParameterExpression poco, PocoColumn column)
        {
            // Start by assuming the database wants the same type that the property is and that we'll just be getting the value from the property:
            // poco.SomeFieldOrProp
            Expression getValueFromPoco = Expression.MakeMemberAccess(poco, column.MemberInfo);
            if (column.MemberInfoType == column.ColumnType) 
                return getValueFromPoco;

            // See if there is a converter available for between the two types
            Delegate converter = _typeConverter.GetToDbConverter(column.MemberInfoType, column.ColumnType);
            if (converter == null)
            {
                // No converter available, at least try a cast:
                // (TColumn) poco.SomeFieldOrProp
                return Expression.Convert(getValueFromPoco, column.ColumnType);
            }
            
            // Invoke the converter:
            // converter(poco.SomeFieldOrProp)
            return Expression.Call(converter.Target == null ? null : Expression.Constant(converter.Target), GetMethod(converter), getValueFromPoco);
        }

        public object AdaptValue(PocoData pocoData, PocoColumn column, object value)
        {
            if (column.MemberInfoType == column.ColumnType)
            {
                return value;
            }
            // See if there is a converter available for between the two types
            var converter =  _typeConverter.GetToDbConverter(column.MemberInfoType, column.ColumnType);
            if (converter == null)
            {
                // No converter available, at least try a cast:
                // (TColumn) poco.SomeFieldOrProp
                return Convert.ChangeType(value, column.ColumnType);
            }
            return converter.DynamicInvoke(value);
        }

        /// <summary>
        /// Gets an Expression that represents calling Row.GetValue&lt;T&gt;(columnIndex) and applying any type conversion necessary to
        /// convert it to the destination type on the POCO.
        /// </summary>
        private Expression GetExpressionToGetColumnValueFromRow(ParameterExpression row, CqlColumn dbColumn, Type pocoDestType)
        {
            // Row.GetValue<T>(columnIndex)
            ConstantExpression columnIndex = Expression.Constant(dbColumn.Index, IntType);
            MethodCallExpression getValueT = Expression.Call(row, GetValueOfTMethod.MakeGenericMethod(dbColumn.Type), columnIndex);

            if (pocoDestType == dbColumn.Type)
            {
                // No casting/conversion needed since the types match exactly
                return getValueT;
            }

            // Check for a converter
            Expression convertedValue;
            Delegate converter = _typeConverter.TryGetFromDbConverter(dbColumn.Type, pocoDestType);
            if (converter == null)
            {
                // No converter is available but the types don't match, so attempt to do:
                //     (TFieldOrProp) row.GetValue<T>(columnIndex);
                try
                {
                    convertedValue = Expression.Convert(getValueT, pocoDestType);
                }
                catch (InvalidOperationException ex)
                {
                    var message = string.Format("It is not possible to convert column `{0}` of type {1} to target type {2}", dbColumn.Name, dbColumn.TypeCode, pocoDestType.Name);
                    throw new InvalidTypeException(message, ex);
                }
            }
            else
            {
                // Invoke the converter function on getValueT (taking into account whether it's a static method):
                //     converter(row.GetValue<T>(columnIndex));
                convertedValue = 
                    Expression.Convert(
                        Expression.Call(
                            converter.Target == null ? null : Expression.Constant(converter.Target), 
                            GetMethod(converter), 
                            getValueT), 
                        pocoDestType);
            }
            // Cassandra will return null for empty collections, so make an effort to populate collection properties on the POCO with
            // empty collections instead of null in those cases
            if (!TryGetCreateEmptyCollectionExpression(dbColumn, pocoDestType, out Expression defaultValue))
            {
                // poco.SomeFieldOrProp = ... createEmptyCollection ...
                defaultValue = Expression.Default(pocoDestType);
            }

            return Expression.Condition(Expression.Call(row, IsNullMethod, columnIndex), defaultValue, convertedValue);
        }

        /// <summary>
        /// Tries to get an Expression that will create an empty collection compatible with the POCO column's type if the type coming from
        /// the database is a collection type.  Returns true if successful, along with the Expression in an out parameter.
        /// </summary>
        private static bool TryGetCreateEmptyCollectionExpression(CqlColumn dbColumn, Type pocoDestType, out Expression createEmptyCollection)
        {
            createEmptyCollection = null;

            // If the DB column isn't a collection type, just bail
            if (IsCassandraCollection(dbColumn) == false)
                return false;

            // See if the POCO's type if something we can create an empty collection for
            if (!pocoDestType.GetTypeInfo().IsInterface)
            {
                // If an array, we know we have a constructor available
                if (pocoDestType.IsArray)
                {
                    // new T[] { }
                    createEmptyCollection = Expression.NewArrayInit(pocoDestType.GetElementType());
                    return true;
                }

                // Is a type implementing ICollection<T>? (this covers types implementing ISet<T>, IDictionary<T> as well)
                if (ImplementsCollectionInterface(pocoDestType))
                {
                    try
                    {
                        // new T();
                        createEmptyCollection = Expression.New(pocoDestType);
                        return true;
                    }
                    catch (ArgumentException)
                    {
                        // Type does not have an empty constructor, so just bail
                        return false;
                    }
                }
            }
            else
            {
                // See if destination type interface on the POCO is one we can create an empty object for
                if (!pocoDestType.GetTypeInfo().IsGenericType)
                {
                    return false;
                }

                Type openGenericType = pocoDestType.GetGenericTypeDefinition();

                // Handle IDictionary<T, U>
                if (openGenericType == typeof (IDictionary<,>))
                {
                    // The driver currently uses SortedDictionary so we will too
                    Type dictionaryType = typeof (SortedDictionary<,>).MakeGenericType(pocoDestType.GetTypeInfo().GetGenericArguments());

                    // (IDictionary<T, U>) new SortedDictionary<T, U>();
                    createEmptyCollection = Expression.Convert(Expression.New(dictionaryType), pocoDestType);
                    return true;
                }

                // Handle ISet<T>
                if (openGenericType == typeof (ISet<>))
                {
                    // The driver uses List (?!) but we'll use a sorted set since that's the CQL semantics
                    Type setType = typeof (SortedSet<>).MakeGenericType(pocoDestType.GetTypeInfo().GetGenericArguments());

                    // (ISet<T>) new SortedSet<T>();
                    createEmptyCollection = Expression.Convert(Expression.New(setType), pocoDestType);
                    return true;
                }

                // Handle interfaces implemented by List<T>, like ICollection<T>, IList<T>, IReadOnlyList<T>, IReadOnlyCollection<T> and IEnumerable<T>
                if (TypeConverter.ListGenericInterfaces.Contains(openGenericType))
                {
                    // The driver uses List so we'll use that as well
                    Type listType = typeof (List<>).MakeGenericType(pocoDestType.GetTypeInfo().GetGenericArguments());

                    // (... IList<T> or ICollection<T> or IEnumerable<T> ...) new List<T>();
                    createEmptyCollection = Expression.Convert(Expression.New(listType), pocoDestType);
                    return true;
                }
            }

            // We don't know what to do to create an empty collection or we don't know it's a collection
            return false;
        }

        /// <summary>
        /// Returns true if the CqlColumn is a collection type.
        /// </summary>
        private static bool IsCassandraCollection(CqlColumn dbColumn)
        {
            return dbColumn.TypeCode == ColumnTypeCode.List || dbColumn.TypeCode == ColumnTypeCode.Set || dbColumn.TypeCode == ColumnTypeCode.Map;
        }

        /// <summary>
        /// Returns true if the Type implements the ICollection&lt;T&gt; interface.
        /// </summary>
        private static bool ImplementsCollectionInterface(Type t)
        {
            return t.GetTypeInfo().GetInterfaces().FirstOrDefault(i => i.GetTypeInfo().IsGenericType && i.GetGenericTypeDefinition() == typeof (ICollection<>)) != null;
        }

        private static MethodInfo GetMethod(Delegate deleg)
        {
            return deleg.Method;
        }
    }
}