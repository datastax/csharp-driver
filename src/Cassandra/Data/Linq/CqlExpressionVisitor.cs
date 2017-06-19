﻿//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Mapping.Utils;

namespace Cassandra.Data.Linq
{
    internal class CqlExpressionVisitor : ExpressionVisitor
    {
        /// <summary>
        /// The initial capacity for query string builders.
        /// </summary>
        private const int DefaultQueryStringCapacity = 128;

        /// <summary>
        /// The initial capacity for WHERE and SET string builders.
        /// </summary>
        private const int DefaultClauseStringCapacity = 64;

        /// <summary>
        /// The initial capacity for WHERE and SET list parameters
        /// </summary>
        private const int DefaultClauseParameterCapacity = 8;

        private readonly string _utf8MaxValue = Encoding.UTF8.GetString(new byte[] { 0xF4, 0x8F, 0xBF, 0xBF });

        private static readonly Dictionary<ExpressionType, string> CqlTags = new Dictionary<ExpressionType, string>
        {
            {ExpressionType.Not, "NOT"},
            {ExpressionType.And, "AND"},
            {ExpressionType.AndAlso, "AND"},
            {ExpressionType.Equal, "="},
            {ExpressionType.NotEqual, "<>"},
            {ExpressionType.GreaterThan, ">"},
            {ExpressionType.GreaterThanOrEqual, ">="},
            {ExpressionType.LessThan, "<"},
            {ExpressionType.LessThanOrEqual, "<="}
        };

        private static readonly HashSet<ExpressionType> CqlUnsupTags = new HashSet<ExpressionType>
        {
            ExpressionType.Or,
            ExpressionType.OrElse
        };

        private static readonly Dictionary<ExpressionType, ExpressionType> CqlInvTags = new Dictionary<ExpressionType, ExpressionType>
        {
            {ExpressionType.Equal, ExpressionType.Equal},
            {ExpressionType.NotEqual, ExpressionType.NotEqual},
            {ExpressionType.GreaterThan, ExpressionType.LessThan},
            {ExpressionType.GreaterThanOrEqual, ExpressionType.LessThanOrEqual},
            {ExpressionType.LessThan, ExpressionType.GreaterThan},
            {ExpressionType.LessThanOrEqual, ExpressionType.GreaterThanOrEqual}
        };

        private readonly VisitingParam<string> _binaryExpressionTag = new VisitingParam<string>();
        private readonly VisitingParam<string> _currentBindingName = new VisitingParam<string>();
        /// <summary>
        /// The active condition (WHERE or UPDATE IF conditions)
        /// </summary>
        private readonly VisitingParam<Tuple<StringBuilder, List<object>>> _currentCondition;
        private readonly VisitingParam<ParsePhase> _parsePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        private readonly PocoData _pocoData;
        private bool _allowFiltering;
        private int _limit;
        private readonly List<Tuple<PocoColumn, object, ExpressionType>> _projections = new List<Tuple<PocoColumn, object, ExpressionType>>();
        private readonly List<Tuple<string, bool>> _orderBy = new List<Tuple<string, bool>>();
        private readonly List<string> _groupBy = new List<string>();
        private readonly List<string> _selectFields = new List<string>(DefaultClauseParameterCapacity);
        
        /// <summary>
        /// Represents a pair composed by cql string and the parameters for the WHERE clause
        /// </summary>
        private readonly Tuple<StringBuilder, List<object>> _whereClause = Tuple.Create(
            new StringBuilder(DefaultClauseStringCapacity), new List<object>(DefaultClauseParameterCapacity));

        /// <summary>
        /// Represents a pair composed by cql string and the parameters for the WHERE clause
        /// </summary>
        private readonly Tuple<StringBuilder, List<object>> _updateIfClause = Tuple.Create(
            new StringBuilder(DefaultClauseStringCapacity), new List<object>(DefaultClauseParameterCapacity));

        private readonly string _tableName;
        private readonly string _keyspaceName;
        private bool _isSelectQuery;

        public CqlExpressionVisitor(PocoData pocoData, string tableName, string keyspaceName)
        {
            if (tableName == null)
            {
                throw new ArgumentNullException("tableName");
            }
            _pocoData = pocoData;
            _tableName = tableName;
            _keyspaceName = keyspaceName;
            _currentCondition = new VisitingParam<Tuple<StringBuilder, List<object>>>(_whereClause);
        }

        /// <summary>
        /// Gets a cql SELECT statement based on the current state
        /// </summary>
        public string GetSelect(Expression expression, out object[] values)
        {
            _isSelectQuery = true;
            Visit(expression);
            var query = new StringBuilder(DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("SELECT ");
            query.Append(_selectFields.Count == 0
                ? _pocoData.Columns.Select(c => Escape(c.ColumnName)).ToCommaDelimitedString()
                : _selectFields.Select(Escape).ToCommaDelimitedString());

            query.Append(" FROM ");
            query.Append(GetEscapedTableName());

            if (_whereClause.Item1.Length > 0)
            {
                query.Append(" WHERE ");
                query.Append(_whereClause.Item1);
                parameters.AddRange(_whereClause.Item2);
            }

            if (_groupBy.Count > 0)
            {
                query.Append(" GROUP BY ");
                query.Append(string.Join(", ", _groupBy));
            }

            if (_orderBy.Count > 0)
            {
                query.Append(" ORDER BY ");
                query.Append(string.Join(", ", _orderBy.Select(item => Escape(item.Item1) + (item.Item2 ? "" : " DESC"))));
            }

            if (_limit > 0)
            {
                query.Append(" LIMIT ?");
                parameters.Add(_limit);
            }

            if (_allowFiltering || _pocoData.AllowFiltering)
            {
                query.Append(" ALLOW FILTERING");
            }
            values = parameters.ToArray();
            return query.ToString();
        }

        /// <summary>
        /// Escapes an identifier if necessary
        /// </summary>
        private string Escape(string identifier)
        {
            if (!_pocoData.CaseSensitive)
            {
                return identifier;
            }
            return "\"" + identifier + "\"";
        }

        /// <summary>
        /// Gets a cql DELETE statement based on the current state
        /// </summary>
        public string GetDelete(Expression expression, out object[] values, DateTimeOffset? timestamp, bool ifExists)
        {
            Visit(expression);
            var query = new StringBuilder(DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("DELETE FROM ");
            query.Append(GetEscapedTableName());
            if (timestamp != null)
            {
                query.Append(" USING TIMESTAMP ?");
                parameters.Add((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
            }

            if (_whereClause.Item1.Length > 0)
            {
                query.Append(" WHERE ");
                query.Append(_whereClause.Item1);
                parameters.AddRange(_whereClause.Item2);
            }

            if (ifExists)
            {
                query.Append(" IF EXISTS ");
            }
            if (_updateIfClause.Item1.Length > 0)
            {
                if (ifExists)
                {
                    throw new CqlArgumentException("IfExits and DeleteIf are mutually excusive,");
                }
                query.Append(" IF ");
                query.Append(_updateIfClause.Item1);
                parameters.AddRange(_updateIfClause.Item2);
            }

            if (_selectFields.Count > 0)
            {
                throw new CqlArgumentException("Can not project result of a DELETE query or delete entity partially");
            }
            values = parameters.ToArray();
            return query.ToString();
        }

        /// <summary>
        /// Gets a cql UPDATE statement based on the current state
        /// </summary>
        public string GetUpdate(Expression expression, out object[] values, int? ttl, DateTimeOffset? timestamp, 
                                MapperFactory mapperFactory)
        {
            Visit(expression);
            var query = new StringBuilder(DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("UPDATE ");
            query.Append(GetEscapedTableName());
            if (ttl != null || timestamp != null)
            {
                query.Append(" USING ");
            }
            if (ttl != null)
            {
                query.Append("TTL ?");
                parameters.Add(ttl.Value);
                if (timestamp != null)
                {
                    query.Append(" AND ");
                }
            }
            if (timestamp != null)
            {
                query.Append("TIMESTAMP ? ");
                parameters.Add((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
            }
            query.Append(" SET ");
            var setStatements = new List<string>();
            foreach (var projection in _projections)
            {
                var column = projection.Item1;
                var columnName = Escape(column.ColumnName);
                var value = mapperFactory.AdaptValue(_pocoData, column, projection.Item2);
                string operation;
                switch (projection.Item3)
                {
                    case ExpressionType.AddAssign:
                        operation = " = " + columnName + " + ?";
                        parameters.Add(value);
                        break;
                    case ExpressionType.PreIncrementAssign:
                        operation = " = ? + " + columnName;
                        parameters.Add(value);
                        break;
                    case ExpressionType.SubtractAssign:
                        operation = " = " + columnName + " - ?";
                        parameters.Add(value);
                        break;
                    case ExpressionType.Increment:
                        //Counter
                        operation = " = " + columnName + " + ?";
                        parameters.Add(projection.Item2);
                        break;
                    case ExpressionType.Decrement:
                        //Counter
                        operation = " = " + columnName + " - ?";
                        parameters.Add(projection.Item2);
                        break;
                    default:
                        operation = " = ?";
                        parameters.Add(value);
                        break;
                }
                setStatements.Add(columnName + operation);
            }

            if (setStatements.Count == 0)
            {
                throw new CqlArgumentException("Nothing to update");
            }
            query.Append(String.Join(", ", setStatements));

            if (_whereClause.Item1.Length > 0)
            {
                query.Append(" WHERE ");
                query.Append(_whereClause.Item1);
                parameters.AddRange(_whereClause.Item2);
            }

            if (_updateIfClause.Item1.Length > 0)
            {
                query.Append(" IF ");
                query.Append(_updateIfClause.Item1);
                parameters.AddRange(_updateIfClause.Item2);
            }
            values = parameters.ToArray();
            return query.ToString();
        }

        public string GetCount(Expression expression, out object[] values)
        {
            Visit(expression);
            var query = new StringBuilder(DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("SELECT count(*) FROM ");
            query.Append(GetEscapedTableName());

            if (_whereClause.Item1.Length > 0)
            {
                query.Append(" WHERE ");
                query.Append(_whereClause.Item1);
                parameters.AddRange(_whereClause.Item2);
            }
            if (_limit > 0)
            {
                query.Append(" LIMIT ?");
                parameters.Add(_limit);
            }

            if (_allowFiltering || _pocoData.AllowFiltering)
            {
                query.Append(" ALLOW FILTERING");
            }

            values = parameters.ToArray();

            return query.ToString();
        }

        private string GetEscapedTableName()
        {
            string name = null;
            if (_keyspaceName != null)
            {
                name = Escape(_keyspaceName) + ".";
            }
            name += Escape(_tableName);
            return name;
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if (_parsePhase.Get() != ParsePhase.SelectBinding)
            {
                throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
            }
            
            // Visit new instance creation (constructor and parameters)
            VisitNew(node.NewExpression);

            foreach (var binding in node.Bindings)
            {
                if (!(binding is MemberAssignment))
                {
                    continue;
                }
                using (_currentBindingName.Set(binding.Member.Name))
                {
                    Visit((binding as MemberAssignment).Expression);
                }
            }
            return node;
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            if (_parsePhase.Get() != ParsePhase.Select)
            {
                return base.VisitLambda(node);
            }
            using (_parsePhase.Set(ParsePhase.SelectBinding))
            {
                using (_currentBindingName.Set(node.Parameters[0].Name))
                {
                    Visit(node.Body);
                }
            }
            return node;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            var phase = _parsePhase.Get();
            if (phase != ParsePhase.SelectBinding && phase != ParsePhase.GroupBy)
            {
                throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
            }
            for (var i = 0; i < node.Arguments.Count; i++)
            {
                var binding = node.Arguments[i];
                if (binding.NodeType == ExpressionType.Parameter)
                {
                    throw new CqlLinqNotSupportedException(binding, phase);
                }
                if (binding.NodeType == ExpressionType.New)
                {
                    //Its a projection constructing a new instance
                    return AddProjection(node);
                }

                string bindingName;
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (node.Members != null && i < node.Members.Count)
                {
                    bindingName = node.Members[i].Name;
                }
                else
                {
                    var memberExpression = binding as MemberExpression;
                    if (memberExpression == null)
                    {
                        throw new CqlLinqNotSupportedException(binding, _parsePhase.Get());
                    }
                    bindingName = memberExpression.Member.Name;
                }

                using (_currentBindingName.Set(bindingName))
                {
                    Visit(binding);
                }
            }
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var initialPhase = _parsePhase.Get();
            switch (node.Method.Name)
            {
                case "Select":
                    Visit(node.Arguments[0]);
                    using (_parsePhase.Set(ParsePhase.Select))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "Where":
                    Visit(node.Arguments[0]);
                    using (_parsePhase.Set(ParsePhase.Condition))
                    {
                        if (_whereClause.Item1.Length != 0)
                        {
                            _whereClause.Item1.Append(" AND ");
                        }
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "UpdateIf":
                case "DeleteIf":
                    Visit(node.Arguments[0]);
                    using (_parsePhase.Set(ParsePhase.Condition))
                    {
                        if (_updateIfClause.Item1.Length != 0)
                        {
                            _updateIfClause.Item1.Append(" AND ");
                        }
                        using (_currentCondition.Set(_updateIfClause))
                        {
                            Visit(node.Arguments[1]);
                        }
                    }
                    return node;
                case "Take":
                    Visit(node.Arguments[0]);
                    using (_parsePhase.Set(ParsePhase.Take))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "GroupBy":
                    Visit(node.Arguments[0]);
                    using (_parsePhase.Set(ParsePhase.GroupBy))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "OrderBy":
                case "ThenBy":
                    Visit(node.Arguments[0]);
                    using (_parsePhase.Set(ParsePhase.OrderBy))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "OrderByDescending":
                case "ThenByDescending":
                    Visit(node.Arguments[0]);
                    using (_parsePhase.Set(ParsePhase.OrderByDescending))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "FirstOrDefault":
                case "First":
                    Visit(node.Arguments[0]);
                    if (node.Arguments.Count == 3)
                    {
                        using (_parsePhase.Set(ParsePhase.Condition))
                        {
                            Visit(node.Arguments[2]);
                        }
                    }
                    _limit = 1;
                    return node;
                case "AllowFiltering":
                    Visit(node.Arguments[0]);
                    _allowFiltering = true;
                    return node;
                case "Min":
                case "Max":
                case "Average":
                case "Sum":
                case "Count":
                    return FillAggregate(initialPhase, node);
            }

            if (initialPhase == ParsePhase.Condition)
            {
                return EvaluateConditionFunction(node);
            }
            if (initialPhase == ParsePhase.SelectBinding)
            {
                if (EvaluateOperatorMethod(node))
                {
                    //Applied operators and functions to UPDATE or SELECT statement
                    return node;
                }
                //Try to evaluate the expression
                return AddProjection(node);
            }

            throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
        }

        /// <summary>
        /// Fill the SELECT field
        /// </summary>
        private Expression FillAggregate(ParsePhase phase, MethodCallExpression node)
        {
            if (phase != ParsePhase.SelectBinding || !_isSelectQuery)
            {
                throw new CqlLinqNotSupportedException(node, phase);
            }
            if (node.Arguments.Count == 2)
            {
                var cqlFunction = node.Method.Name == "Average" ? "AVG" : node.Method.Name;
                Visit(node.Arguments[1]);
                if (_selectFields.Count == 0)
                {
                    // The selected field should be populated by now
                    throw new CqlLinqNotSupportedException(node, phase);
                }
                var index = _selectFields.Count - 1;
                _selectFields[index] = cqlFunction.ToUpperInvariant() + "(" + _selectFields[index] + ")";
            }
            else
            {
                _selectFields.Add("COUNT(*)");
            }
            return node;
        }

        /// <summary>
        /// Tries to evaluate the current expression and add it as a projection
        /// </summary>
        private Expression AddProjection(Expression node, PocoColumn column = null)
        {
            object value;
            if (node is MemberExpression)
            {
                value = GetClosureValue((MemberExpression) node);
            }
            else
            {
                value = Expression.Lambda(node).Compile().DynamicInvoke();
            }
            if (column == null)
            {
                column = _pocoData.GetColumnByMemberName(_currentBindingName.Get());
                if (column == null)
                {
                    throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
                } 
            }
            _projections.Add(Tuple.Create(column, value, ExpressionType.Assign));
            return node;
        }

        private Expression EvaluateConditionFunction(MethodCallExpression node)
        {
            var methodName = node.Method.Name;
            if (node.Method.DeclaringType != null)
            {
                if (node.Method.DeclaringType == typeof(CqlToken))
                {
                    methodName = "CqlToken";
                }
            }
            if (EvaluateMethod(methodName, node))
            {
                //It was evaluated as one of the methods
                return node;
            }
            //Try to invoke to obtain the value
            var val = Expression.Lambda(node).Compile().DynamicInvoke();
            _currentCondition.Get().Item2.Add(val);
            _currentCondition.Get().Item1.Append("?");
            return node;
        }

        private bool EvaluateMethod(string name, MethodCallExpression node)
        {
            var clause = _currentCondition.Get().Item1;
            var parameters = _currentCondition.Get().Item2;
            switch (name)
            {
                case "Contains":
                {
                    Expression what;
                    Expression inp;
                    if (node.Object == null)
                    {
                        what = node.Arguments[1];
                        inp = node.Arguments[0];
                    }
                    else
                    {
                        what = node.Arguments[0];
                        inp = node.Object;
                    }
                    Visit(what);
                    var values = (IEnumerable)Expression.Lambda(inp).Compile().DynamicInvoke();
                    var placeHolders = new StringBuilder();
                    foreach (var v in values)
                    {
                        placeHolders.Append(placeHolders.Length == 0 ? "?" : ", ?");
                        parameters.Add(v);
                    }

                    clause
                        .Append(" IN (")
                        .Append(placeHolders)
                        .Append(")");
                    return true;
                }
                case "StartsWith":
                    Visit(node.Object);
                    var startsWithArgument = node.Arguments[0];
                    var startString = (string)Expression.Lambda(startsWithArgument).Compile().DynamicInvoke();
                    var endString = startString + _utf8MaxValue;

                    parameters.Add(startString);
                    parameters.Add(endString);

                    clause
                        .Append(" ")
                        .Append(CqlTags[ExpressionType.GreaterThanOrEqual])
                        .Append(" ? ")
                        .Append(CqlTags[ExpressionType.And])
                        .Append(" ");

                    Visit(node.Object);

                    clause
                        .Append(" ")
                        .Append(CqlTags[ExpressionType.LessThan])
                        .Append(" ?");
                    
                    return true;
                case "CompareTo":
                    Visit(node.Object);
                    clause
                        .Append(" ")
                        .Append(_binaryExpressionTag.Get())
                        .Append(" ");
                    Visit(node.Arguments[0]);
                    return true;
                case "Equals":
                    Visit(node.Object);
                    clause.Append(" = ");
                    Visit(node.Arguments[0]);
                    return true;
                case "CqlToken":
                case "Token":
                    clause.Append("token(");
                    var tokenArgs = node.Arguments;
                    for (var i = 0; i < tokenArgs.Count; i++)
                    {
                        var arg = tokenArgs[i];
                        if (i > 0)
                        {
                            clause.Append(", ");   
                        }
                        Visit(arg);
                    }
                    clause.Append(")");
                    return true;
                case "MaxTimeUuid":
                case "MinTimeUuid":
                    clause.Append(name.ToLowerInvariant()).Append("(");
                    Visit(node.Arguments[0]);
                    clause.Append(")");
                    return true;
            }
            return false;
        }

        private bool EvaluateOperatorMethod(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof (CqlOperator))
            {
                return false;
            }
            var column = _pocoData.GetColumnByMemberName(_currentBindingName.Get());
            object value = Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke();
            ExpressionType expressionType;
            switch (node.Method.Name)
            {
                case "Append":
                    expressionType = ExpressionType.AddAssign;
                    break;
                case "Prepend":
                    expressionType = ExpressionType.PreIncrementAssign;
                    break;
                case "SubstractAssign":
                    expressionType = ExpressionType.SubtractAssign;
                    break;
                default:
                    return false;
            }
            _projections.Add(Tuple.Create(column, value, expressionType));
            return true;
        }

        private static Expression DropNullableConversion(Expression node)
        {
            if (node is UnaryExpression && node.NodeType == ExpressionType.Convert && node.Type.GetTypeInfo().IsGenericType &&
                String.Compare(node.Type.Name, "Nullable`1", StringComparison.Ordinal) == 0)
            {
                return (node as UnaryExpression).Operand;
            }
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (_parsePhase.Get() == ParsePhase.Condition)
            {
                var clause = _currentCondition.Get().Item1;
                var parameters = _currentCondition.Get().Item2;
                if (CqlTags.ContainsKey(node.NodeType))
                {
                    clause.Append(CqlTags[node.NodeType] + " (");
                    Visit(DropNullableConversion(node.Operand));
                    clause.Append(")");
                }
                else if (node.NodeType == ExpressionType.Convert)
                {
                    Visit(node.Operand);
                }
                else
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    parameters.Add(val);
                    clause.Append("?");
                }
                return node;
            }
            if (_parsePhase.Get() == ParsePhase.SelectBinding)
            {
                if (node.NodeType == ExpressionType.Convert && node.Type.Name == "Nullable`1")
                {
                    return Visit(node.Operand);
                }
                var column = _pocoData.GetColumnByMemberName(_currentBindingName.Get());
                if (column != null && column.IsCounter)
                {
                    var value = Expression.Lambda(node).Compile().DynamicInvoke();
                    if (!(value is long || value is int))
                    {
                        throw new ArgumentException("Only Int64 and Int32 values are supported as counter increment of decrement values");
                    }
                    _projections.Add(Tuple.Create(column, value, ExpressionType.Increment));
                    _selectFields.Add(column.ColumnName);
                    return node;
                }

            }
            throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
        }

        private static bool IsCompareTo(Expression node)
        {
            if (node.NodeType == ExpressionType.Call)
            {
                var methodCallExpression = node as MethodCallExpression;
                if (methodCallExpression != null && methodCallExpression.Method.Name == "CompareTo")
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsZero(Expression node)
        {
            if (node.NodeType == ExpressionType.Constant)
            {
                var constantExpression = node as ConstantExpression;
                if (constantExpression == null || !(constantExpression.Value is int))
                {
                    return false;
                }
                if (Convert.ToInt32((node as ConstantExpression).Value) == 0)
                {
                    return true;
                }
            }
            return false;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (_parsePhase.Get() == ParsePhase.Condition)
            {
                if (CqlTags.ContainsKey(node.NodeType))
                {
                    if (IsCompareTo(node.Left))
                    {
                        if (IsZero(node.Right))
                        {
                            using (_binaryExpressionTag.Set(CqlTags[node.NodeType]))
                            {
                                Visit(node.Left);
                            }
                            return node;
                        }
                    }
                    else if (IsCompareTo(node.Right))
                    {
                        if (IsZero(node.Left))
                        {
                            using (_binaryExpressionTag.Set(CqlTags[CqlInvTags[node.NodeType]]))
                            {
                                Visit(node.Right);
                            }
                            return node;
                        }
                    }
                    else
                    {
                        Visit(DropNullableConversion(node.Left));
                        _currentCondition.Get().Item1.Append(" " + CqlTags[node.NodeType] + " ");
                        Visit(DropNullableConversion(node.Right));
                        return node;
                    }
                }
                else if (!CqlUnsupTags.Contains(node.NodeType))
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    _currentCondition.Get().Item2.Add(val);
                    _currentCondition.Get().Item1.Append("?");
                    return node;
                }
            }
            else if (_parsePhase.Get() == ParsePhase.SelectBinding)
            {
                var column = _pocoData.GetColumnByMemberName(_currentBindingName.Get());
                if (column == null)
                {
                    throw new ArgumentException("Trying to select a column does it excluded in the mappings");
                }
                AddProjection(node, column);
                _selectFields.Add(column.ColumnName);
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is ITable)
            {
                return node;
            }
            switch (_parsePhase.Get())
            {
                case ParsePhase.Condition:
                    _currentCondition.Get().Item1.Append("?");
                    _currentCondition.Get().Item2.Add(node.Value);
                    return node;
                case ParsePhase.SelectBinding:
                {
                    var column = _pocoData.GetColumnByMemberName(_currentBindingName.Get());
                    if (column == null)
                    {
                        //selecting a field that is not part of PocoType
                        break;
                    }
                    var expressionType = ExpressionType.Assign;
                    if (column.IsCounter)
                    {
                        if (!(node.Value is long || node.Value is int))
                        {
                            throw new ArgumentException("Only Int64 and Int32 values are supported as counter increment of decrement values");
                        }
                        expressionType = ExpressionType.Increment;
                    }
                    _projections.Add(Tuple.Create(column, node.Value, expressionType));
                    _selectFields.Add(column.ColumnName);
                    return node;
                }
                case ParsePhase.Take:
                    _limit = (int) node.Value;
                    return node;
                case ParsePhase.OrderBy:
                case ParsePhase.OrderByDescending:
                {
                    var columnName = _pocoData.GetColumnNameByMemberName((string) node.Value);
                    if (columnName == null)
                    {
                        //order by a field that is not part of PocoType
                        break;
                    }
                    _orderBy.Add(Tuple.Create(columnName, _parsePhase.Get() == ParsePhase.OrderBy));
                    return node;
                }

            }
            throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            switch (_parsePhase.Get())
            {
                case ParsePhase.Condition:
                    return FillCondition(node, _currentCondition.Get().Item1, _currentCondition.Get().Item2);
                case ParsePhase.SelectBinding:
                    if (!_isSelectQuery)
                    {
                        return FillUpdateProjection(node);
                    }
                    return FillSelectProjection(node);
                case ParsePhase.OrderByDescending:
                case ParsePhase.OrderBy:
                    return FillOrderBy(node);
                case ParsePhase.GroupBy:
                    return FillGroupBy(node);
            }
            throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
        }

        private Expression FillCondition(MemberExpression node, StringBuilder clause, List<object> parameters)
        {
            if (node.Expression == null || node.Expression.NodeType == ExpressionType.MemberAccess)
            {
                var val = GetClosureValue(node);
                parameters.Add(val);
                clause.Append("?");
                return node;
            }
            if (node.Expression.NodeType == ExpressionType.Parameter)
            {
                var columnName = _pocoData.GetColumnName(node.Member);
                if (columnName == null)
                {
                    throw new InvalidOperationException(
                        "Trying to order by a field or property that is ignored or not part of the mapping definition.");
                }
                clause.Append(Escape(columnName));
                return node;
            }
            if (node.Expression.NodeType == ExpressionType.Constant)
            {
                var val = GetClosureValue(node);
                if (val is CqlToken)
                {
                    clause.Append("token(");
                    var tokenValues = (val as CqlToken).Values;
                    var tokenPlaceholders = new StringBuilder(tokenValues.Length * 3);
                    foreach (var pk in tokenValues)
                    {
                        parameters.Add(pk);
                        tokenPlaceholders.Append(tokenPlaceholders.Length == 0 ? "?" : ", ?");
                    }
                    clause.Append(")");
                }
                else
                {
                    parameters.Add(val);
                    clause.Append("?");
                }
                return node;
            }
            return node;
        }

        private Expression FillSelectProjection(MemberExpression node)
        {
            var column = _pocoData.GetColumnByMemberName(node.Member.Name);
            if (column == null)
            {
                // DeclaringType is IGrouping<,>
                var declaringType = node.Member.DeclaringType.GetTypeInfo();
                if (_groupBy.Count != 1 || !declaringType.IsGenericType ||
                    declaringType.GetGenericTypeDefinition() != typeof(IGrouping<,>))
                {
                    throw new InvalidOperationException("No mapping defined for member: " + node.Member.Name);
                }
                // The single field in the GROUP BY is being selected
                _selectFields.Add(_groupBy[0]);
                return node;
            }
            _selectFields.Add(column.ColumnName);
            return node;
        }

        private Expression FillUpdateProjection(MemberExpression node)
        {
            PocoColumn column;
            if (node.Expression == null || node.Expression.NodeType != ExpressionType.Parameter)
            {
                column = _pocoData.GetColumnByMemberName(_currentBindingName.Get());
                if (column == null)
                {
                    throw new InvalidOperationException("No mapping defined for member: " + node.Member.Name);
                }
                if (column.IsCounter)
                {
                    var value = GetClosureValue(node);
                    if (!(value is long || value is int))
                    {
                        throw new ArgumentException("Only Int64 and Int32 values are supported as counter increment of decrement values");
                    }
                    _projections.Add(Tuple.Create(column, value, ExpressionType.Increment));
                    return node;
                }
                AddProjection(node, column);
                return node;
            }
            column = _pocoData.GetColumnByMemberName(node.Member.Name);
            if (column == null)
            {
                throw new InvalidOperationException("No mapping defined for member: " + node.Member.Name);
            }
            _projections.Add(Tuple.Create(column, (object)column.ColumnName, ExpressionType.Assign));
            return node;
        }

        private Expression FillOrderBy(MemberExpression node)
        {
            var columnName = _pocoData.GetColumnName(node.Member);
            if (columnName == null)
            {
                throw new InvalidOperationException(
                    "Trying to order by a field or property that is ignored or not part of the mapping definition.");
            }
            _orderBy.Add(Tuple.Create(columnName, _parsePhase.Get() == ParsePhase.OrderBy));
            if ((node.Expression is ConstantExpression))
            {
                return node;
            }
            if (node.Expression.NodeType == ExpressionType.Parameter)
            {
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
        }

        private Expression FillGroupBy(MemberExpression node)
        {
            var columnName = _pocoData.GetColumnName(node.Member);
            if (columnName == null)
            {
                throw new InvalidOperationException(string.Format("Trying to group by a field or property that " +
                    "is ignored or not part of the mapping definition: {0}", node.Member.Name));
            }
            _groupBy.Add(columnName);
            if ((node.Expression is ConstantExpression) || node.Expression.NodeType == ExpressionType.Parameter)
            {
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
        }

        private static object GetClosureValue(MemberExpression node)
        {
            object value;
            if (node.Member.MemberType == MemberTypes.Field)
            {
                value = GetFieldValue(node);
            }
            else if (node.Member.MemberType == MemberTypes.Property)
            {
                value = GetPropertyValue(node);
            }
            else
            {
                value = Expression.Lambda(node).Compile().DynamicInvoke();
            }
            return value;
        }

        private static object GetFieldValue(MemberExpression node)
        {
            var fieldInfo = (FieldInfo)node.Member;
            if (node.Expression is MemberExpression)
            {
                // The field of a field instance
                var instance = GetFieldValue((MemberExpression)node.Expression);
                return fieldInfo.GetValue(instance);
            }
            if (node.Expression == null)
            {
                // Static field
                return fieldInfo.GetValue(null);
            }
            return fieldInfo.GetValue(((ConstantExpression)node.Expression).Value);
        }

        private static object GetPropertyValue(MemberExpression node)
        {
            var propertyInfo = (PropertyInfo)node.Member;
            if (node.Expression is MemberExpression)
            {
                // Field property
                var instance = GetFieldValue((MemberExpression)node.Expression);
                return propertyInfo.GetValue(instance, null);
            }
            // Current instance property
            return propertyInfo.GetValue(((ConstantExpression)node.Expression).Value, null);
        }
    }
}
