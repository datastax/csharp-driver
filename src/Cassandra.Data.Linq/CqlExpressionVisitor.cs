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

        private readonly VisitingParam<string> _binaryExpressionTag = new VisitingParam<string>(null);
        private readonly CqlStringTool _cqlTool = new CqlStringTool();
        private readonly VisitingParam<string> _currentBindingName = new VisitingParam<string>(null);
        private readonly VisitingParam<StringBuilder> _currentConditionBuilder;
        private readonly VisitingParam<ParsePhase> _phasePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        private readonly PocoData _pocoData;
        private bool _allowFiltering;
        private readonly Dictionary<string, object> _projections = new Dictionary<string, object>();
        private int _limit;
        private readonly List<string> _orderBy = new List<string>();
        private string _quotedTableName;
        private readonly List<string> _selectFields = new List<string>();
        private readonly StringBuilder _updateIfClause = new StringBuilder();
        private readonly StringBuilder _whereClause = new StringBuilder();

        public CqlExpressionVisitor(PocoData pocoData)
        {
            _pocoData = pocoData;
            _currentConditionBuilder = new VisitingParam<StringBuilder>(_whereClause);
        }

        public string GetSelect(out object[] values, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT ");
            sb.Append(_selectFields.Count == 0 ? "*" : string.Join(", ", _selectFields));

            sb.Append(" FROM ");
            sb.Append(_quotedTableName);

            if (_whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(_whereClause);
            }

            if (_orderBy.Count > 0)
            {
                sb.Append(" ORDER BY ");
                sb.Append(string.Join(", ", _orderBy));
            }

            if (_limit > 0)
            {
                sb.Append(" LIMIT ");
                sb.Append(_limit);
            }

            if (_allowFiltering)
                sb.Append(" ALLOW FILTERING");

            if (withValues)
                return _cqlTool.FillWithValues(sb.ToString(), out values);
            values = null;
            return _cqlTool.FillWithEncoded(sb.ToString());
        }


        public string GetDelete(out object[] values, DateTimeOffset? timestamp, bool ifExists = false, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("DELETE FROM ");
            sb.Append(_quotedTableName);
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
                return _cqlTool.FillWithValues(sb.ToString(), out values);
            else
            {
                values = null;
                return _cqlTool.FillWithEncoded(sb.ToString());
            }
        }

        public string GetUpdate(out object[] values, int? ttl, DateTimeOffset? timestamp, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("UPDATE ");
            sb.Append(_quotedTableName);
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
                if (projection.Value == null)
                {
                    setStatements.Add(projection.Key + " = NULL");
                    continue;
                }
                setStatements.Add(projection.Key + " = " + _cqlTool.AddValue(projection.Value));
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
            sb.Append(_quotedTableName);

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
            if (_phasePhase.get() == ParsePhase.SelectBinding)
            {
                foreach (MemberBinding binding in node.Bindings)
                {
                    if (binding is MemberAssignment)
                    {
                        using (_currentBindingName.set(binding.Member.Name))
                            Visit((binding as MemberAssignment).Expression);
                    }
                }
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.get());
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            if (_phasePhase.get() == ParsePhase.What)
            {
                using (_phasePhase.set(ParsePhase.SelectBinding))
                using (_currentBindingName.set(node.Parameters[0].Name))
                    Visit(node.Body);
                return node;
            }
            return base.VisitLambda(node);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            if (_phasePhase.get() != ParsePhase.SelectBinding)
            {
                throw new CqlLinqNotSupportedException(node, _phasePhase.get());
            }
            for (var i = 0; i < node.Members.Count; i++)
            {
                var binding = node.Arguments[i];
                if (binding.NodeType == ExpressionType.Parameter)
                {
                    throw new CqlLinqNotSupportedException(binding, _phasePhase.get());
                }
                using (_currentBindingName.set(node.Members[i].Name))
                {
                    Visit(binding);
                }
            }
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Select")
            {
                Visit(node.Arguments[0]);

                using (_phasePhase.set(ParsePhase.What))
                    Visit(node.Arguments[1]);

                return node;
            }
            if (node.Method.Name == "Where")
            {
                Visit(node.Arguments[0]);

                using (_phasePhase.set(ParsePhase.Condition))
                {
                    if (_whereClause.Length != 0)
                        _whereClause.Append(" AND ");
                    Visit(node.Arguments[1]);
                }
                return node;
            }
            if (node.Method.Name == "UpdateIf" || node.Method.Name == "DeleteIf")
            {
                this.Visit(node.Arguments[0]);

                using (_phasePhase.set(ParsePhase.Condition))
                {
                    if (_updateIfClause.Length != 0)
                        _updateIfClause.Append(" AND ");
                    using (_currentConditionBuilder.set(_updateIfClause))
                        this.Visit(node.Arguments[1]);
                }
                return node;
            }
            if (node.Method.Name == "Take")
            {
                Visit(node.Arguments[0]);
                using (_phasePhase.set(ParsePhase.Take))
                    Visit(node.Arguments[1]);
                return node;
            }
            if (node.Method.Name == "OrderBy" || node.Method.Name == "ThenBy")
            {
                Visit(node.Arguments[0]);
                using (_phasePhase.set(ParsePhase.OrderBy))
                    Visit(node.Arguments[1]);
                return node;
            }
            if (node.Method.Name == "OrderByDescending" || node.Method.Name == "ThenByDescending")
            {
                Visit(node.Arguments[0]);
                using (_phasePhase.set(ParsePhase.OrderByDescending))
                    Visit(node.Arguments[1]);
                return node;
            }
            if (node.Method.Name == "FirstOrDefault" || node.Method.Name == "First")
            {
                Visit(node.Arguments[0]);
                if (node.Arguments.Count == 3)
                {
                    using (_phasePhase.set(ParsePhase.Condition))
                        Visit(node.Arguments[2]);
                }
                _limit = 1;
                return node;
            }

            if (_phasePhase.get() == ParsePhase.Condition)
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
                    _currentConditionBuilder.get().Append(" IN (");
                    var values = (IEnumerable) Expression.Lambda(inp).Compile().DynamicInvoke();
                    bool first = false;
                    foreach (object obj in values)
                    {
                        if (!first)
                            first = true;
                        else
                            _currentConditionBuilder.get().Append(", ");
                        _currentConditionBuilder.get().Append(_cqlTool.AddValue(obj));
                    }
                    _currentConditionBuilder.get().Append(")");
                    return node;
                }
                if (node.Method.Name == "CompareTo")
                {
                    Visit(node.Object);
                    _currentConditionBuilder.get().Append(" " + _binaryExpressionTag.get() + " ");
                    Visit(node.Arguments[0]);
                    return node;
                }
                if (node.Method.Name == "Equals")
                {
                    Visit(node.Object);
                    _currentConditionBuilder.get().Append(" = ");
                    Visit(node.Arguments[0]);
                    return node;
                }
                if (node.Type.Name == "CqlToken")
                {
                    _currentConditionBuilder.get().Append("token(");
                    ReadOnlyCollection<Expression> exprs = node.Arguments;
                    Visit(exprs.First());
                    foreach (Expression e in exprs.Skip(1))
                    {
                        _currentConditionBuilder.get().Append(", ");
                        Visit(e);
                    }
                    _currentConditionBuilder.get().Append(")");
                    return node;
                }
                object val = Expression.Lambda(node).Compile().DynamicInvoke();
                _currentConditionBuilder.get().Append(_cqlTool.AddValue(val));
                return node;
            }
            if (node.Method.Name == "AllowFiltering")
            {
                Visit(node.Arguments[0]);

                _allowFiltering = true;
                return node;
            }

            throw new CqlLinqNotSupportedException(node, _phasePhase.get());
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
            if (_phasePhase.get() == ParsePhase.Condition)
            {
                if (CqlTags.ContainsKey(node.NodeType))
                {
                    _currentConditionBuilder.get().Append(CqlTags[node.NodeType] + " (");
                    Visit(DropNullableConversion(node.Operand));
                    _currentConditionBuilder.get().Append(")");
                }
                else
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    _currentConditionBuilder.get().Append(_cqlTool.AddValue(val));
                }
                return node;
            }
            if (_phasePhase.get() == ParsePhase.SelectBinding)
            {
                if (node.NodeType == ExpressionType.Convert && node.Type.Name == "Nullable`1")
                {
                    return Visit(node.Operand);
                }
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.get());
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
            if (_phasePhase.get() == ParsePhase.Condition)
            {
                if (CqlTags.ContainsKey(node.NodeType))
                {
                    if (IsCompareTo(node.Left))
                    {
                        if (IsZero(node.Right))
                        {
                            using (_binaryExpressionTag.set(CqlTags[node.NodeType]))
                                Visit(node.Left);
                            return node;
                        }
                    }
                    else if (IsCompareTo(node.Right))
                    {
                        if (IsZero(node.Left))
                        {
                            using (_binaryExpressionTag.set(CqlTags[CqlInvTags[node.NodeType]]))
                                Visit(node.Right);
                            return node;
                        }
                    }
                    else
                    {
                        Visit(DropNullableConversion(node.Left));
                        _currentConditionBuilder.get().Append(" " + CqlTags[node.NodeType] + " ");
                        Visit(DropNullableConversion(node.Right));
                        return node;
                    }
                }
                else if (!CqlUnsupTags.Contains(node.NodeType))
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    _currentConditionBuilder.get().Append(_cqlTool.AddValue(val));
                    return node;
                }
            }
            else if (_phasePhase.get() == ParsePhase.SelectBinding)
            {
                var val = Expression.Lambda(node).Compile().DynamicInvoke();
                var columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.get());
                _projections[columnName] = val;
                _selectFields.Add(columnName);
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.get());
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is ITable)
            {
                var table = (node.Value as ITable);
                //TODO: Check case sensitivity
                _quotedTableName = _pocoData.TableName;
                //TODO: Replace
                _allowFiltering = table.GetEntityType().GetCustomAttributes(typeof (AllowFilteringAttribute), false).Any();
                return node;
            }
            if (_phasePhase.get() == ParsePhase.Condition)
            {
                _currentConditionBuilder.get().Append(_cqlTool.AddValue(node.Value));
                return node;
            }
            if (_phasePhase.get() == ParsePhase.SelectBinding)
            {
                var columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.get());
                _projections[columnName] = node.Value;
                _selectFields.Add(_currentBindingName.get());
                return node;
            }
            if (_phasePhase.get() == ParsePhase.Take)
            {
                _limit = (int) node.Value;
                return node;
            }
            if (_phasePhase.get() == ParsePhase.OrderBy || _phasePhase.get() == ParsePhase.OrderByDescending)
            {
                var columnName = _pocoData.GetColumnNameByMemberName((string) node.Value);
                //TODO: Quote after
                _orderBy.Add(columnName + (_phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));
                return node;
            }
            throw new CqlLinqNotSupportedException(node, _phasePhase.get());
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            switch (_phasePhase.get())
            {
                case ParsePhase.Condition:
                    if (node.Expression == null)
                    {
                        object val = Expression.Lambda(node).Compile().DynamicInvoke();
                        _currentConditionBuilder.get().Append(_cqlTool.AddValue(val));
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Parameter)
                    {
                        _currentConditionBuilder.get().Append(_pocoData.GetColumnName(node.Member));
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.Constant)
                    {
                        var val = Expression.Lambda(node).Compile().DynamicInvoke();
                        if (val is CqlToken)
                        {
                            _currentConditionBuilder.get().Append("token(");
                            _currentConditionBuilder.get().Append(_cqlTool.AddValue((val as CqlToken).Values.First()));
                            foreach (object e in (val as CqlToken).Values.Skip(1))
                            {
                                _currentConditionBuilder.get().Append(", ");
                                _currentConditionBuilder.get().Append(_cqlTool.AddValue(e));
                            }
                            _currentConditionBuilder.get().Append(")");
                        }
                        else
                        {
                            _currentConditionBuilder.get().Append(_cqlTool.AddValue(val));
                        }
                        return node;
                    }
                    if (node.Expression.NodeType == ExpressionType.MemberAccess)
                    {
                        object val = Expression.Lambda(node).Compile().DynamicInvoke();
                        _currentConditionBuilder.get().Append(_cqlTool.AddValue(val));
                        return node;
                    }
                    break;
                case ParsePhase.SelectBinding:
                {
                    var columnName = _pocoData.GetColumnName(node.Member);
                    if (node.Expression == null)
                    {
                        var value = Expression.Lambda(node).Compile().DynamicInvoke();
                        columnName = _pocoData.GetColumnNameByMemberName(_currentBindingName.get());
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
                    _orderBy.Add(_pocoData.GetColumnName(node.Member) + (_phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));
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
            throw new CqlLinqNotSupportedException(node, _phasePhase.get());
        }
    }
}
