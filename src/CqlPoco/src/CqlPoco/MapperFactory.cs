using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra;
using CqlPoco.Statements;

namespace CqlPoco
{
    internal class MapperFactory
    {
        private static readonly Type CassandraRowType = typeof (Row);

        // Not pretty, but here we're getting the Row.GetValue<T>(int column) instance method
        private static readonly MethodInfo GetValueOfTMethod =
            CassandraRowType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                            .Where(mi => mi.Name == "GetValue" && mi.IsGenericMethodDefinition)
                            .Select(
                                mi => new {MethodInfo = mi, GenericArgsCount = mi.GetGenericArguments().Length, MethodParams = mi.GetParameters()})
                            .Where(
                                mi => mi.GenericArgsCount == 1 && mi.MethodParams.Length == 1 && mi.MethodParams[0].ParameterType == typeof (int))
                            .Select(mi => mi.MethodInfo)
                            .Single();

        private readonly PocoData _pocoData;
        private readonly ConcurrentDictionary<string, Delegate> _cache;

        public MapperFactory(PocoData pocoData)
        {
            if (pocoData == null) throw new ArgumentNullException("pocoData");
            _pocoData = pocoData;

            _cache = new ConcurrentDictionary<string, Delegate>();
        }

        /// <summary>
        /// Gets a mapper Func that can map from a C* row to the POCO type T for the given statement.
        /// </summary>
        public Func<Row, T> GetMapper<T>(IStatementWrapper statement, RowSet rows)
        {
            string key = statement.Cql;
            Delegate mapperFunc = _cache.GetOrAdd(key, _ => CreateMapper<T>(rows));
            return (Func<Row, T>) mapperFunc;
        }
        
        private Func<Row, T> CreateMapper<T>(RowSet rows)
        {
            // We're going to store the method body expressions in a list since we need to use some looping to generate it
            var methodBodyExpressions = new List<Expression>();
            
            // The input parameter for our Func<Row, T>, a C* Row
            ParameterExpression row = Expression.Parameter(CassandraRowType, "row");
            
            // T poco = new T();
            ParameterExpression poco = Expression.Variable(_pocoData.PocoType, "poco");
            methodBodyExpressions.Add(Expression.Assign(poco, Expression.New(_pocoData.PocoType)));       // TODO: Constructor selection?
            
            foreach (CqlColumn column in rows.Columns)
            {
                // Try to find a corresponding column on the POCO and if not found, don't map that column from the RowSet
                PocoColumn pocoColumn;
                if (_pocoData.Columns.TryGetValue(column.Name, out pocoColumn) == false)
                    continue;

                // getValueT = row.GetValue<T>(columnIndex)
                ConstantExpression columnIndex = Expression.Constant(column.Index, typeof(int));
                MethodCallExpression getValueT = Expression.Call(row, GetValueOfTMethod.MakeGenericMethod(column.Type), columnIndex);

                // If the destination type of the property/field on the POCO doesn't match the type coming from the database, add a conversion
                Expression getValue;
                if (pocoColumn.MemberInfoType != column.Type)
                {
                    // (TPropOrField) row.GetValue<T>(columnIndex);
                    getValue = Expression.Convert(getValueT, pocoColumn.MemberInfoType);
                }
                else
                {
                    // row.GetValue<T>(columnIndex);
                    getValue = getValueT;
                }

                // poco.SomeFieldOrProp = ... getValue ...
                var setField = Expression.Assign(Expression.MakeMemberAccess(poco, pocoColumn.MemberInfo), getValue);
                methodBodyExpressions.Add(setField);
            }

            // The last expression in the method body is the return value, so put our new POCO at the end
            methodBodyExpressions.Add(poco);

            // Create a block expression for the method body expressions
            BlockExpression methodBody = Expression.Block(new [] {poco}, methodBodyExpressions);
            
            // Return compiled expression
            return Expression.Lambda<Func<Row, T>>(methodBody, row).Compile();
        }
    }
}
