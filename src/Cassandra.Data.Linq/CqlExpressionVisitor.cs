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
using Cassandra.Mapping.Mapping;

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
        private readonly VisitingParam<StringBuilder> _currentCondition;
        private readonly VisitingParam<ParsePhase> _phasePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        private readonly CqlStringTool _cqlTool = new CqlStringTool();
        private readonly PocoData _pocoData;
        private bool _allowFiltering;
        private int _limit;
        private readonly Dictionary<string, object> _projections = new Dictionary<string, object>();
        private readonly List<Tuple<string, bool>> _orderBy = new List<Tuple<string, bool>>();
        private readonly List<string> _selectFields = new List<string>();
        private readonly StringBuilder _updateIfClause = new StringBuilder();
        private readonly StringBuilder _whereClause = new StringBuilder();

        public CqlExpressionVisitor(PocoData pocoData)
        {
            _pocoData = pocoData;
            _currentCondition = new VisitingParam<StringBuilder>(_whereClause);
        }

        /// <summary>
        /// Gets a cql SELECT statement based on the current state
        /// </summary>
        public string GetSelect(out object[] values, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT ");
            if (_selectFields.Count == 0)
            {
                //Select all columns
                sb.Append("*");
            }
            else
            {
                sb.Append(String.Join(", ", _selectFields.Select(Escape)));   
            }

            sb.Append(" FROM ");
            sb.Append(Escape(_pocoData.TableName));

            if (_whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(_whereClause);
            }

            if (_orderBy.Count > 0)
            {
                sb.Append(" ORDER BY ");
                sb.Append(string.Join(", ", _orderBy.Select(item => Escape(item.Item1) + (item.Item2 ? "" : " DESC"))));
            }

            if (_limit > 0)
            {
                sb.Append(" LIMIT ");
                sb.Append(_limit);
            }

            if (_allowFiltering)
            {
                sb.Append(" ALLOW FILTERING");
            }

            if (withValues)
            {
                return _cqlTool.FillWithValues(sb.ToString(), out values);
            }
            values = null;
            return _cqlTool.FillWithEncoded(sb.ToString());
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
        public string GetDelete(out object[] values, DateTimeOffset? timestamp, bool ifExists = false, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("DELETE FROM ");
            sb.Append(Escape(_pocoData.TableName));
            if (timestamp != null)
            {
                sb.Append(" USING TIMESTAMP ");
                sb.Append((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
                sb.Append(" ");
            }

            if (_whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(_whereClause);
            }

            if (ifExists)
            {
                sb.Append(" IF EXISTS ");
            }

            if (_updateIfClause.Length > 0)
            {
                if (ifExists)
                    throw new CqlArgumentException("IfExits and DeleteIf are mutually excusive,");

                sb.Append(" IF ");
                sb.Append(_updateIfClause);
            }

            if (_selectFields.Count > 0)
                throw new CqlArgumentException("Unable to delete entity partially");

            if (withValues)
            {
                return _cqlTool.FillWithValues(sb.ToString(), out values);
            }
            values = null;
            return _cqlTool.FillWithEncoded(sb.ToString());
        }

        /// <summary>
        /// Gets a cql UPDATE statement based on the current state
        /// </summary>
        public string GetUpdate(out object[] values, int? ttl, DateTimeOffset? timestamp, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("UPDATE ");
            sb.Append(Escape(_pocoData.TableName));
            if (ttl != null || timestamp != null)
            {
                sb.Append(" USING ");
            }
            if (ttl != null)
            {
                sb.Append("TTL ");
                sb.Append(ttl.Value);
                if (timestamp != null)
                    sb.Append(" AND ");
            }
            if (timestamp != null)
            {
                sb.Append("TIMESTAMP ");
                sb.Append((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
                sb.Append(" ");
            }
            sb.Append(" SET ");

            var setStatements = new List<string>();
            foreach (var projection in _projections)
            {
                var columnName = Escape(projection.Key);
                if (projection.Value == null)
                {
                    setStatements.Add(columnName + " = NULL");
                    continue;
                }
                setStatements.Add(columnName + " = " + _cqlTool.AddValue(projection.Value));
            }

            if (setStatements.Count == 0)
            {
                throw new CqlArgumentException("Nothing to update");
            }
            sb.Append(String.Join(", ", setStatements));

            if (_whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(_whereClause);
            }

            if (_updateIfClause.Length > 0)
            {
                sb.Append(" IF ");
                sb.Append(_updateIfClause);
            }

            if (withValues)
                return _cqlTool.FillWithValues(sb.ToString(), out values);
            values = null;
            return _cqlTool.FillWithEncoded(sb.ToString());
        }

        public string GetCount(out object[] values, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT count(*) FROM ");
            sb.Append(Escape(_pocoData.TableName));

            if (_whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(_whereClause);
            }

            if (_limit > 0)
            {
                sb.Append(" LIMIT ");
                sb.Append(_limit);
            }

            if (withValues)
                return _cqlTool.FillWithValues(sb.ToString(), out values);
            values = null;
            return _cqlTool.FillWithEncoded(sb.ToString());
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
            for (var i = 0; i < node.Members.Count; i++)
            {
                var binding = node.Arguments[i];
                if (binding.NodeType == ExpressionType.Parameter)
                {
                    throw new CqlLinqNotSupportedException(binding, _phasePhase.Get());
                }
                using (_currentBindingName.Set(node.Members[i].Name))
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
                        if (_whereClause.Length != 0)
                        {
                            _whereClause.Append(" AND ");
                        }
                        Visit(node.Arguments[1]);
                    }
                    return node;
                case "UpdateIf":
                case "DeleteIf":
                    Visit(node.Arguments[0]);
                    using (_phasePhase.Set(ParsePhase.Condition))
                    {
                        if (_updateIfClause.Length != 0)
                        {
                            _updateIfClause.Append(" AND ");
                        }
                        using (_currentCondition.Set(_updateIfClause))
                        {
                            this.Visit(node.Arguments[1]);
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
            if (node.Method.Name == "Contains")
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
                _currentCondition.Get().Append(" IN (");
                var values = (IEnumerable)Expression.Lambda(inp).Compile().DynamicInvoke();
                bool first = false;
                foreach (object obj in values)
                {
                    if (!first)
                        first = true;
                    else
                        _currentCondition.Get().Append(", ");
                    _currentCondition.Get().Append(_cqlTool.AddValue(obj));
                }
                _currentCondition.Get().Append(")");
                return node;
            }
            if (node.Method.Name == "CompareTo")
            {
                Visit(node.Object);
                _currentCondition.Get().Append(" " + _binaryExpressionTag.Get() + " ");
                Visit(node.Arguments[0]);
                return node;
            }
            if (node.Method.Name == "Equals")
            {
                Visit(node.Object);
                _currentCondition.Get().Append(" = ");
                Visit(node.Arguments[0]);
                return node;
            }
            if (node.Type.Name == "CqlToken")
            {
                _currentCondition.Get().Append("token(");
                ReadOnlyCollection<Expression> exprs = node.Arguments;
                Visit(exprs.First());
                foreach (Expression e in exprs.Skip(1))
                {
                    _currentCondition.Get().Append(", ");
                    Visit(e);
                }
                _currentCondition.Get().Append(")");
                return node;
            }
            var val = Expression.Lambda(node).Compile().DynamicInvoke();
            _currentCondition.Get().Append(_cqlTool.AddValue(val));
            return node;
        }

        private static Expression DropNullableConversion(Expression node)
        {
            if (node is UnaryExpression && node.NodeType == ExpressionType.Convert && node.Type.IsGenericType &&
                node.Type.Name.CompareTo("Nullable`1") == 0)
                return (node as UnaryExpression).Operand;
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (_phasePhase.Get() == ParsePhase.Condition)
            {
                if (CqlTags.ContainsKey(node.NodeType))
                {
                    _currentCondition.Get().Append(CqlTags[node.NodeType] + " (");
                    Visit(DropNullableConversion(node.Operand));
                    _currentCondition.Get().Append(")");
                }
                else
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    _currentCondition.Get().Append(_cqlTool.AddValue(val));
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

        private bool IsCompareTo(Expression node)
        {
            if (node.NodeType == ExpressionType.Call)
                if ((node as MethodCallExpression).Method.Name == "CompareTo")
                    return true;
            return false;
        }

        private bool IsZero(Expression node)
        {
            if (node.NodeType == ExpressionType.Constant)
                if ((node as ConstantExpression).Value is int)
                    if (((int) (node as ConstantExpression).Value) == 0)
                        return true;
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
                                Visit(node.Left);
                            return node;
                        }
                    }
                    else if (IsCompareTo(node.Right))
                    {
                        if (IsZero(node.Left))
                        {
                            using (_binaryExpressionTag.Set(CqlTags[CqlInvTags[node.NodeType]]))
                                Visit(node.Right);
                            return node;
                        }
                    }
                    else
                    {
                        Visit(DropNullableConversion(node.Left));
                        _currentCondition.Get().Append(" " + CqlTags[node.NodeType] + " ");
                        Visit(DropNullableConversion(node.Right));
                        return node;
                    }
                }
                else if (!CqlUnsupTags.Contains(node.NodeType))
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    _currentCondition.Get().Append(_cqlTool.AddValue(val));
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
                var table = (node.Value as ITable);
                //TODO: Replace
                _allowFiltering = table.GetEntityType().GetCustomAttributes(typeof (AllowFilteringAttribute), false).Any();
                return node;
            }
            if (_phasePhase.Get() == ParsePhase.Condition)
            {
                _currentCondition.Get().Append(_cqlTool.AddValue(node.Value));
                return node;
            }
            if (_phasePhase.Get() == ParsePhase.SelectBinding)
            {
                var columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.Get());
                _projections[columnName] = node.Value;
                _selectFields.Add(columnName);
                return node;
            }
            if (_phasePhase.Get() == ParsePhase.Take)
            {
                _limit = (int) node.Value;
                return node;
            }
            if (_phasePhase.Get() == ParsePhase.OrderBy || _phasePhase.Get() == ParsePhase.OrderByDescending)
            {
                var columnName = _pocoData.GetColumnNameByMemberName((string) node.Value);
                _orderBy.Add(Tuple.Create(columnName, _phasePhase.Get() == ParsePhase.OrderBy));
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            switch (_phasePhase.Get())
            {
                case ParsePhase.Condition:
                    if (node.Expression == null)
                    {
                        object val = Expression.Lambda(node).Compile().DynamicInvoke();
                        _currentCondition.Get().Append(_cqlTool.AddValue(val));
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Parameter)
                    {
                        var columnName = Escape(_pocoData.GetColumnName(node.Member));
                        _currentCondition.Get().Append(columnName);
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Constant)
                    {
                        var val = Expression.Lambda(node).Compile().DynamicInvoke();
                        if (val is CqlToken)
                        {
                            _currentCondition.Get().Append("token(");
                            _currentCondition.Get().Append(_cqlTool.AddValue((val as CqlToken).Values.First()));
                            foreach (object e in (val as CqlToken).Values.Skip(1))
                            {
                                _currentCondition.Get().Append(", ");
                                _currentCondition.Get().Append(_cqlTool.AddValue(e));
                            }
                            _currentCondition.Get().Append(")");
                        }
                        else
                        {
                            _currentCondition.Get().Append(_cqlTool.AddValue(val));
                        }
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.MemberAccess)
                    {
                        object val = Expression.Lambda(node).Compile().DynamicInvoke();
                        _currentCondition.Get().Append(_cqlTool.AddValue(val));
                        return node;
                    }
                    break;
                case ParsePhase.SelectBinding:
                {
                    var columnName = _pocoData.GetColumnName(node.Member);
                    if (node.Expression == null)
                    {
                        var value = Expression.Lambda(node).Compile().DynamicInvoke();
                        columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.Get());
                        _projections[columnName] = value;
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Constant || node.Expression.NodeType == ExpressionType.MemberAccess)
                    {
                        var value = Expression.Lambda(node.Expression).Compile().DynamicInvoke();
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
                }
                    break;
                case ParsePhase.OrderByDescending:
                case ParsePhase.OrderBy:
                    _orderBy.Add(Tuple.Create(_pocoData.GetColumnName(node.Member), _phasePhase.Get() == ParsePhase.OrderBy));
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
            throw new CqlLinqNotSupportedException(node, _phasePhase.Get());
        }
    }
}
