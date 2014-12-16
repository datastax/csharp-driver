//
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
using System.Collections.ObjectModel;
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
            ExpressionType.OrElse,
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
        private readonly VisitingParam<ParsePhase> _phasePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        private readonly PocoData _pocoData;
        private bool _allowFiltering;
        private int _limit;
        private readonly Dictionary<string, object> _projections = new Dictionary<string, object>();
        private readonly List<Tuple<string, bool>> _orderBy = new List<Tuple<string, bool>>();
        private readonly List<string> _selectFields = new List<string>();
        /// <summary>
        /// Represents a pair composed by cql string and the parameters for the WHERE clause
        /// </summary>
        private readonly Tuple<StringBuilder, List<object>> _whereClause = Tuple.Create(new StringBuilder(), new List<object>());
        /// <summary>
        /// Represents a pair composed by cql string and the parameters for the WHERE clause
        /// </summary>
        private readonly Tuple<StringBuilder, List<object>> _updateIfClause = Tuple.Create(new StringBuilder(), new List<object>());

        private readonly string _tableName;
        private readonly string _keyspaceName;

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
        public string GetSelect(out object[] values)
        {
            var query = new StringBuilder();
            var parameters = new List<object>();
            query.Append("SELECT ");
            if (_selectFields.Count == 0)
            {
                //Select all columns
                query.Append("*");
            }
            else
            {
                query.Append(String.Join(", ", _selectFields.Select(Escape)));   
            }

            query.Append(" FROM ");
            query.Append(GetEscapedTableName());

            if (_whereClause.Item1.Length > 0)
            {
                query.Append(" WHERE ");
                query.Append(_whereClause.Item1);
                parameters.AddRange(_whereClause.Item2);
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
        /// Escapes an identier if necessary
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
        public string GetDelete(out object[] values, DateTimeOffset? timestamp, bool ifExists)
        {
            var query = new StringBuilder();
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
        public string GetUpdate(out object[] values, int? ttl, DateTimeOffset? timestamp)
        {
            var query = new StringBuilder();
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
                setStatements.Add(Escape(projection.Key) + " = ?");
                parameters.Add(projection.Value);
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

        public string GetCount(out object[] values)
        {
            var query = new StringBuilder();
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

        public string GetInsert<T>(T poco, bool ifNotExists, int? ttl, DateTimeOffset? timestamp, List<object> parameters)
        {
            var query = new StringBuilder();
            var columns = _pocoData.Columns.Select(c => Escape(c.ColumnName)).ToCommaDelimitedString();
            var placeholders = Enumerable.Repeat("?", _pocoData.Columns.Count).ToCommaDelimitedString();
            query.Append(String.Format("INSERT INTO {0} ({1}) VALUES ({2})", GetEscapedTableName(), columns, placeholders));

            if (ifNotExists)
            {
                query.Append(" IF NOT EXISTS");
            }
            if (ttl != null || timestamp != null)
            {
                query.Append(" USING");
                if (ttl != null)
                {
                    query.Append(" TTL ?");
                    parameters.Add(ttl.Value);
                    if (timestamp != null)
                    {
                        query.Append(" AND");
                    }
                }
                if (timestamp != null)
                {
                    query.Append(" TIMESTAMP ?");
                    parameters.Add(timestamp.Value);
                }
            }

            return query.ToString();
        }

        public void Evaluate(Expression expression)
        {
            Visit(expression);
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if (_phasePhase.Get() != ParsePhase.SelectBinding)
            {
                throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
            }
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
            if (_phasePhase.Get() != ParsePhase.What)
            {
                return base.VisitLambda(node);
            }
            using (_phasePhase.Set(ParsePhase.SelectBinding))
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
            if (_phasePhase.Get() != ParsePhase.SelectBinding)
            {
                throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
            }
            for (var i = 0; i < node.Arguments.Count; i++)
            {
                var binding = node.Arguments[i];
                if (binding.NodeType == ExpressionType.Parameter)
                {
                    throw new CqlLinqNotSupportedException(binding, _phasePhase.Get());
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
                        throw new CqlLinqNotSupportedException(binding, _phasePhase.Get());
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
            switch (node.Method.Name)
            {
                case "Select":
                    Visit(node.Arguments[0]);
                    using (_phasePhase.Set(ParsePhase.What))
                        Visit(node.Arguments[1]);
                    return node;
                case "Where":
                    Visit(node.Arguments[0]);
                    using (_phasePhase.Set(ParsePhase.Condition))
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
                    using (_phasePhase.Set(ParsePhase.Condition))
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
                    using (_phasePhase.Set(ParsePhase.Take))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "OrderBy":
                case "ThenBy":
                    Visit(node.Arguments[0]);
                    using (_phasePhase.Set(ParsePhase.OrderBy))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "OrderByDescending":
                case "ThenByDescending":
                    Visit(node.Arguments[0]);
                    using (_phasePhase.Set(ParsePhase.OrderByDescending))
                    {
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "FirstOrDefault":
                case "First":
                    Visit(node.Arguments[0]);
                    if (node.Arguments.Count == 3)
                    {
                        using (_phasePhase.Set(ParsePhase.Condition))
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
            }

            if (_phasePhase.Get() == ParsePhase.Condition)
            {
                return EvaluateConditionFunction(node);
            }

            throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
        }

        private Expression EvaluateConditionFunction(MethodCallExpression node)
        {
            var methodName = node.Method.Name;
            if (node.Method.ReflectedType != null)
            {
                if (node.Method.ReflectedType == typeof(CqlToken))
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
                    Expression what = null;
                    Expression inp = null;
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

        private static Expression DropNullableConversion(Expression node)
        {
            if (node is UnaryExpression && node.NodeType == ExpressionType.Convert && node.Type.IsGenericType &&
                String.Compare(node.Type.Name, "Nullable`1", StringComparison.Ordinal) == 0)
            {
                return (node as UnaryExpression).Operand;
            }
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (_phasePhase.Get() == ParsePhase.Condition)
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
            if (_phasePhase.Get() == ParsePhase.SelectBinding)
            {
                if (node.NodeType == ExpressionType.Convert && node.Type.Name == "Nullable`1")
                {
                    return Visit(node.Operand);
                }
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
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
            if (_phasePhase.Get() == ParsePhase.Condition)
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
            else if (_phasePhase.Get() == ParsePhase.SelectBinding)
            {
                var val = Expression.Lambda(node).Compile().DynamicInvoke();
                var columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.Get());
                _projections[columnName] = val;
                _selectFields.Add(columnName);
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is ITable)
            {
                return node;
            }
            switch (_phasePhase.Get())
            {
                case ParsePhase.Condition:
                    _currentCondition.Get().Item1.Append("?");
                    _currentCondition.Get().Item2.Add(node.Value);
                    return node;
                case ParsePhase.SelectBinding:
                {
                    var columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.Get());
                    if (columnName == null)
                    {
                        //selecting a field that is not part of PocoType
                        break;
                    }
                    _projections[columnName] = node.Value;
                    _selectFields.Add(columnName);
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
                    _orderBy.Add(Tuple.Create(columnName, _phasePhase.Get() == ParsePhase.OrderBy));
                    return node;
                }

            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            switch (_phasePhase.Get())
            {
                case ParsePhase.Condition:
                {
                    var clause = _currentCondition.Get().Item1;
                    var parameters = _currentCondition.Get().Item2;
                    if (node.Expression == null)
                    {
                        var val = Expression.Lambda(node).Compile().DynamicInvoke();
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
                        var val = Expression.Lambda(node).Compile().DynamicInvoke();
                        if (val is CqlToken)
                        {
                            clause.Append("token(");
                            var tokenPlaceholders = new StringBuilder();
                            foreach (var pk in (val as CqlToken).Values)
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
                    if (node.Expression.NodeType == ExpressionType.MemberAccess)
                    {
                        var val = Expression.Lambda(node).Compile().DynamicInvoke();
                        parameters.Add(val);
                        clause.Append("?");
                        return node;
                    }
                    break;
                }
                case ParsePhase.SelectBinding:
                {
                    var columnName = _pocoData.GetColumnName(node.Member);
                    if (columnName == null)
                    {
                        //Not valid: Trying to select fields that are not part of PocoType
                        break;
                    }
                    if (node.Expression == null)
                    {
                        var value = Expression.Lambda(node).Compile().DynamicInvoke();
                        columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.Get());
                        _projections[columnName] = value;
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Constant || node.Expression.NodeType == ExpressionType.MemberAccess)
                    {
                        var value = Expression.Lambda(node).Compile().DynamicInvoke();
                        _projections[columnName] = value;
                        _selectFields.Add(columnName);
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Parameter)
                    {
                        _projections[columnName] = columnName;
                        _selectFields.Add(columnName);
                        return node;
                    }
                    break;
                }
                case ParsePhase.OrderByDescending:
                case ParsePhase.OrderBy:
                {
                    var columnName = _pocoData.GetColumnName(node.Member);
                    if (columnName == null)
                    {
                        throw new InvalidOperationException(
                            "Trying to order by a field or property that is ignored or not part of the mapping definition.");
                    }
                    _orderBy.Add(Tuple.Create(columnName, _phasePhase.Get() == ParsePhase.OrderBy));
                    if ((node.Expression is ConstantExpression))
                    {
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Parameter)
                    {
                        return node;
                    }
                    break;
                }
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
        }
    }
}
