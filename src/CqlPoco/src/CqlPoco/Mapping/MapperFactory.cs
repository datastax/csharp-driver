using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra;
using CqlPoco.Statements;
using CqlPoco.TypeConversion;

namespace CqlPoco.Mapping
{
    /// <summary>
    /// Component capable of getting/creating Func&lt;Row, T&gt; functions that create POCOs of type T from a Cassandra Row.
    /// </summary>
    internal class MapperFactory
    {
        private static readonly Type CassandraRowType = typeof (Row);
        private const BindingFlags PublicInstance = BindingFlags.Public | BindingFlags.Instance;

        // The Row.GetValue<T>(int column) instance method
        private static readonly MethodInfo GetValueOfTMethod = CassandraRowType.GetMethods(PublicInstance).Single(mi =>
        {
            if (mi.Name != "GetValue" || mi.IsGenericMethodDefinition == false)
                return false;

            Type[] genericArgs = mi.GetGenericArguments();
            if (genericArgs.Length != 1)
                return false;

            ParameterInfo[] parameters = mi.GetParameters();
            return parameters.Length == 1 && parameters[0].ParameterType == typeof (int);
        });

        // The Row.IsNull(int column) instance method
        private static readonly MethodInfo IsNullMethod = CassandraRowType.GetMethods(PublicInstance).Single(mi =>
        {
            if (mi.Name != "IsNull")
                return false;

            ParameterInfo[] parameters = mi.GetParameters();
            return parameters.Length == 1 && parameters[0].ParameterType == typeof (int);
        });

        private readonly TypeConverterFactory _typeConverter;
        private readonly PocoDataFactory _pocoDataFactory;
        private readonly ConcurrentDictionary<Tuple<Type, string>, Delegate> _mapperFuncCache;

        public MapperFactory(TypeConverterFactory typeConverter, PocoDataFactory pocoDataFactory)
        {
            if (typeConverter == null) throw new ArgumentNullException("typeConverter");
            if (pocoDataFactory == null) throw new ArgumentNullException("pocoDataFactory");
            _typeConverter = typeConverter;
            _pocoDataFactory = pocoDataFactory;

            _mapperFuncCache = new ConcurrentDictionary<Tuple<Type, string>, Delegate>();
        }

        /// <summary>
        /// Gets a mapper Func that can map from a C* row to the POCO type T for the given statement.
        /// </summary>
        public Func<Row, T> GetMapper<T>(IStatementWrapper statement, RowSet rows)
        {
            Tuple<Type, string> key = Tuple.Create(typeof (T), statement.Cql);
            Delegate mapperFunc = _mapperFuncCache.GetOrAdd(key, _ => CreateMapper<T>(rows));
            return (Func<Row, T>) mapperFunc;
        }
        
        private Func<Row, T> CreateMapper<T>(RowSet rows)
        {
            PocoData pocoData = _pocoDataFactory.GetPocoData<T>();

            // See if we retrieved only one column and if that column does not exist in the PocoData
            if (rows.Columns.Length == 1 && pocoData.Columns.ContainsKey(rows.Columns[0].Name) == false)
            {
                // Map the single column value directly to the POCO
                return CreateMapperForSingleColumnToPoco<T>(rows, pocoData);
            }
            
            // Create a default POCO mapper
            return CreateMapperForPoco<T>(rows, pocoData);
        }

        private Func<Row, T> CreateMapperForSingleColumnToPoco<T>(RowSet rows, PocoData pocoData)
        {
            ParameterExpression row = Expression.Parameter(CassandraRowType, "row");

            CqlColumn dbColumn = rows.Columns[0];

            LabelTarget returnTarget = Expression.Label(pocoData.PocoType);

            // Get an expression for getting the value of the single column as TPoco (and returning it)
            Expression getColumnValue = Expression.Return(returnTarget, GetExpressionToGetColumnValue(row, dbColumn, pocoData.PocoType));

            // If it is null, try to provide an empty collection for collection types, otherwise do nothing (empty expression)
            Expression ifIsNull = Expression.Empty();

            Expression createEmptyCollection;
            if (TryGetCreateEmptyCollectionExpression(dbColumn, pocoData.PocoType, out createEmptyCollection))
                ifIsNull = Expression.Return(returnTarget, createEmptyCollection);

            // if (row.IsNull(0) == false)
            //     return ... getColumnValue ...
            // else
            //     return ... empty collection or default(TPoco) ...
            var methodBody = Expression.Block(
                Expression.IfThenElse(
                    Expression.IsFalse(Expression.Call(row, IsNullMethod, Expression.Constant(0, typeof (int)))), 
                    getColumnValue, 
                    ifIsNull),
                Expression.Label(returnTarget, Expression.Default(pocoData.PocoType)));

            return Expression.Lambda<Func<Row, T>>(methodBody, row).Compile();
        }

        /// <summary>
        /// Creates a mapper Func for a POCO.
        /// </summary>
        private Func<Row, T> CreateMapperForPoco<T>(RowSet rows, PocoData pocoData)
        {
            // We're going to store the method body expressions in a list since we need to use some looping to generate it
            var methodBodyExpressions = new List<Expression>();

            // The input parameter for our Func<Row, T>, a C* Row
            ParameterExpression row = Expression.Parameter(CassandraRowType, "row");

            // T poco = new T();
            ParameterExpression poco = Expression.Variable(pocoData.PocoType, "poco");
            methodBodyExpressions.Add(Expression.Assign(poco, Expression.New(pocoData.PocoType)));       // TODO: Constructor selection?

            // Keep track of any variables we need in the method body, starting with the poco variable
            var methodBodyVariables = new List<ParameterExpression> { poco };

            foreach (CqlColumn dbColumn in rows.Columns)
            {
                // Try to find a corresponding column on the POCO and if not found, don't map that column from the RowSet
                PocoColumn pocoColumn;
                if (pocoData.Columns.TryGetValue(dbColumn.Name, out pocoColumn) == false)
                    continue;

                // Figure out if we're going to need to do any casting/conversion when we call Row.GetValue<T>(columnIndex)
                Expression getColumnValue = GetExpressionToGetColumnValue(row, dbColumn, pocoColumn.MemberInfoType);
                
                // poco.SomeFieldOrProp = ... getColumnValue call ...
                BinaryExpression ifRowIsNotNull = Expression.Assign(Expression.MakeMemberAccess(poco, pocoColumn.MemberInfo), getColumnValue);

                // Start with an expression that does nothing if the row is null
                Expression ifRowIsNull = Expression.Empty();

                // Cassandra will return null for empty collections, so make an effort to populate collection properties on the POCO with
                // empty collections instead of null in those cases
                Expression createEmptyCollection;
                if (TryGetCreateEmptyCollectionExpression(dbColumn, pocoColumn.MemberInfoType, out createEmptyCollection))
                {
                    // poco.SomeFieldOrProp = ... createEmptyCollection ...
                    ifRowIsNull = Expression.Assign(Expression.MakeMemberAccess(poco, pocoColumn.MemberInfo), createEmptyCollection);
                }
                
                // if (row.IsNull(columnIndex) == false)
                //     ... ifRowIsNotNull ...
                // else
                //     ... ifRowIsNull ...
                var columnIndex = Expression.Constant(dbColumn.Index, typeof (int));
                methodBodyExpressions.Add(Expression.IfThenElse(Expression.IsFalse(Expression.Call(row, IsNullMethod, columnIndex)), ifRowIsNotNull,
                                                                ifRowIsNull));
            }

            // The last expression in the method body is the return value, so put our new POCO at the end
            methodBodyExpressions.Add(poco);

            // Create a block expression for the method body expressions
            BlockExpression methodBody = Expression.Block(methodBodyVariables, methodBodyExpressions);

            // Return compiled expression
            return Expression.Lambda<Func<Row, T>>(methodBody, row).Compile();
        }

        /// <summary>
        /// Gets an Expression that represents calling Row.GetValue&lt;T&gt;(columnIndex) and applying any type conversion necessary to
        /// convert it to the destination type on the POCO.
        /// </summary>
        private Expression GetExpressionToGetColumnValue(ParameterExpression row, CqlColumn dbColumn, Type pocoDestType)
        {
            // Row.GetValue<T>(columnIndex)
            ConstantExpression columnIndex = Expression.Constant(dbColumn.Index, typeof(int));
            MethodCallExpression getValueT = Expression.Call(row, GetValueOfTMethod.MakeGenericMethod(dbColumn.Type), columnIndex);

            if (pocoDestType == dbColumn.Type)
            {
                // No casting/conversion needed since the types match exactly
                return getValueT;
            }
            else
            {
                // If the destination type on the POCO is a nullable type, we're going to take care of null (by checking for it) so
                // we really want to check for a converter to the underlying type
                Type convertToType = Nullable.GetUnderlyingType(pocoDestType) ?? pocoDestType;
                Delegate converter = _typeConverter.GetFromDbConverter(dbColumn.Type, convertToType);
                if (converter == null)
                {
                    // No converter is available but the types don't match, so attempt to do:
                    //     (TFieldOrProp) row.GetValue<T>(columnIndex);
                    return Expression.Convert(getValueT, pocoDestType);
                }
                else
                {
                    // Invoke the converter function on getValueT (taking into account whether it's a static method):
                    //     converter(row.GetValue<T>(columnIndex));
                    Expression getColumnValue = Expression.Call(converter.Target == null ? null : Expression.Constant(converter.Target), converter.Method, getValueT);

                    // If the converter's return type doesn't match (because the destination was a nullable type), add a cast:
                    //     (TFieldOrProp) converter(row.GetValue<T>(columnIndex));
                    if (convertToType != pocoDestType)
                        getColumnValue = Expression.Convert(getColumnValue, pocoDestType);

                    return getColumnValue;
                }
            }
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
            if (pocoDestType.IsInterface == false)
            {
                // If an array, we know we have a constructor available
                if (pocoDestType.IsArray)
                {
                    // new T[] { }
                    createEmptyCollection = Expression.NewArrayInit(pocoDestType);
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
                if (pocoDestType.IsGenericType == false)
                    return false;

                Type openGenericType = pocoDestType.GetGenericTypeDefinition();

                // Handle IDictionary<T, U>
                if (openGenericType == typeof (IDictionary<,>))
                {
                    // The driver currently uses SortedDictionary so we will too
                    Type dictionaryType = typeof (SortedDictionary<,>).MakeGenericType(pocoDestType.GetGenericArguments());

                    // (IDictionary<T, U>) new SortedDictionary<T, U>();
                    createEmptyCollection = Expression.Convert(Expression.New(dictionaryType), pocoDestType);
                    return true;
                }

                // Handle ISet<T>
                if (openGenericType == typeof (ISet<>))
                {
                    // The driver uses List (?!) but we'll use a sorted set since that's the CQL semantics
                    Type setType = typeof (SortedSet<>).MakeGenericType(pocoDestType.GetGenericArguments());

                    // (ISet<T>) new SortedSet<T>();
                    createEmptyCollection = Expression.Convert(Expression.New(setType), pocoDestType);
                    return true;
                }

                // Handle ICollection<T>, IList<T>, and IEnumerable<T>
                if (openGenericType == typeof (ICollection<>) || openGenericType == typeof (IList<>) || openGenericType == typeof (IEnumerable<>))
                {
                    // The driver uses List so we'll use that as well
                    Type listType = typeof (List<>).MakeGenericType(pocoDestType.GetGenericArguments());

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
            return t.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof (ICollection<>)) != null;
        }
    }
}
