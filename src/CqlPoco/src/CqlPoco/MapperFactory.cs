using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra;
using CqlPoco.Statements;
using CqlPoco.TypeConversion;

namespace CqlPoco
{
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

        private readonly ConcurrentDictionary<Tuple<Type, string>, Delegate> _mapperFuncCache;

        private readonly TypeConverterFactory _typeConverter;
        private readonly PocoDataFactory _pocoDataFactory;

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

                // Row.GetValue<T>(columnIndex)
                ConstantExpression columnIndex = Expression.Constant(dbColumn.Index, typeof(int));
                MethodCallExpression getValueT = Expression.Call(row, GetValueOfTMethod.MakeGenericMethod(dbColumn.Type), columnIndex);

                // Figure out if we're going to need to do any casting/conversion when we call Row.GetValue<T>(columnIndex)
                Expression getColumnValue;
                if (pocoColumn.MemberInfoType == dbColumn.Type)
                {
                    // No casting/conversion needed since the types match exactly
                    getColumnValue = getValueT;
                }
                else
                {
                    // If the destination type on the POCO is a nullable type, we're going to take care of null (by checking for it) so
                    // we really want to check for a converter to the underlying type
                    Type convertToType = Nullable.GetUnderlyingType(pocoColumn.MemberInfoType) ?? pocoColumn.MemberInfoType;
                    Delegate converter = _typeConverter.GetFromDbConverter(dbColumn.Type, convertToType);
                    if (converter == null)
                    {
                        // No converter is available but the types don't match, so attempt to do:
                        //     (TFieldOrProp) row.GetValue<T>(columnIndex);
                        getColumnValue = Expression.ConvertChecked(getValueT, pocoColumn.MemberInfoType);
                    }
                    else
                    {
                        // Invoke the converter function on getValueT (taking into account whether it's a static method):
                        //     converter(row.GetValue<T>(columnIndex));
                        getColumnValue = Expression.Call(converter.Target == null ? null : Expression.Constant(converter.Target), converter.Method, getValueT);

                        // If the converter's return type doesn't match (because the destination was a nullable type), add a cast:
                        //     (TFieldOrProp) converter(row.GetValue<T>(columnIndex));
                        if (convertToType != pocoColumn.MemberInfoType)
                            getColumnValue = Expression.ConvertChecked(getColumnValue, pocoColumn.MemberInfoType);
                    }
                }

                // if (row.IsNull(columnIndex) == false)
                //     poco.SomeFieldOrProp = ... getColumnValue call ...
                methodBodyExpressions.Add(Expression.IfThen(Expression.IsFalse(Expression.Call(row, IsNullMethod, columnIndex)),
                                                            Expression.Assign(Expression.MakeMemberAccess(poco, pocoColumn.MemberInfo), getColumnValue)));
            }

            // The last expression in the method body is the return value, so put our new POCO at the end
            methodBodyExpressions.Add(poco);

            // Create a block expression for the method body expressions
            BlockExpression methodBody = Expression.Block(methodBodyVariables, methodBodyExpressions);
            
            // Return compiled expression
            return Expression.Lambda<Func<Row, T>>(methodBody, row).Compile();
        }
    }
}
