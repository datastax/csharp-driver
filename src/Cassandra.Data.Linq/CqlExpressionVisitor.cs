//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra.Data.Linq
{
    internal class CqlExpressionVisitor : ExpressionVisitor
    {
        private static readonly Dictionary<ExpressionType, string> CQLTags = new Dictionary<ExpressionType, string>
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

        private static readonly HashSet<ExpressionType> CQLUnsupTags = new HashSet<ExpressionType>
        {
            ExpressionType.Or,
            ExpressionType.OrElse,
        };

        private static readonly Dictionary<ExpressionType, ExpressionType> CQLInvTags = new Dictionary<ExpressionType, ExpressionType>
        {
            {ExpressionType.Equal, ExpressionType.Equal},
            {ExpressionType.NotEqual, ExpressionType.NotEqual},
            {ExpressionType.GreaterThan, ExpressionType.LessThan},
            {ExpressionType.GreaterThanOrEqual, ExpressionType.LessThanOrEqual},
            {ExpressionType.LessThan, ExpressionType.GreaterThan},
            {ExpressionType.LessThanOrEqual, ExpressionType.GreaterThanOrEqual}
        };

        private readonly VisitingParam<string> binaryExpressionTag = new VisitingParam<string>(null);
        private readonly CqlStringTool cqlTool = new CqlStringTool();
        private readonly VisitingParam<string> currentBindingName = new VisitingParam<string>(null);
        private readonly VisitingParam<StringBuilder> currentConditionBuilder;
        private readonly VisitingParam<ParsePhase> phasePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        public bool AllowFiltering = false;

        public Dictionary<string, string> Alter = new Dictionary<string, string>();
        public int Limit = 0;
        public Dictionary<string, Tuple<string, object, int>> Mappings = new Dictionary<string, Tuple<string, object, int>>();
        public List<string> OrderBy = new List<string>();
        public string QuotedTableName;
        public HashSet<string> SelectFields = new HashSet<string>();
        public StringBuilder UpdateIfClause = new StringBuilder();
        public StringBuilder WhereClause = new StringBuilder();

        public CqlExpressionVisitor()
        {
            currentConditionBuilder = new VisitingParam<StringBuilder>(WhereClause);
        }

        public string GetSelect(out object[] values, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT ");
            sb.Append(SelectFields.Count == 0 ? "*" : string.Join(", ", from f in SelectFields select Alter[f].QuoteIdentifier()));

            sb.Append(" FROM ");
            sb.Append(QuotedTableName);

            if (WhereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(WhereClause);
            }

            if (OrderBy.Count > 0)
            {
                sb.Append(" ORDER BY ");
                sb.Append(string.Join(", ", OrderBy));
            }

            if (Limit > 0)
            {
                sb.Append(" LIMIT ");
                sb.Append(Limit);
            }

            if (AllowFiltering)
                sb.Append(" ALLOW FILTERING");

            if (withValues)
                return cqlTool.FillWithValues(sb.ToString(), out values);
            values = null;
            return cqlTool.FillWithEncoded(sb.ToString());
        }


        public string GetDelete(out object[] values, DateTimeOffset? timestamp, bool ifExists = false, bool withValues = true)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM ");
            sb.Append(QuotedTableName);
            if (timestamp != null)
            {
                sb.Append(" USING TIMESTAMP ");
                sb.Append((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
                sb.Append(" ");
            }

            if (WhereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(WhereClause);
            }

            if (ifExists)
            {
                sb.Append(" IF EXISTS ");
            }

            if (UpdateIfClause.Length > 0)
            {
                if (ifExists)
                    throw new CqlArgumentException("IfExits and DeleteIf are mutually excusive,");

                sb.Append(" IF ");
                sb.Append(UpdateIfClause);
            }

            if (SelectFields.Count > 0)
                throw new CqlArgumentException("Unable to delete entity partially");

            if (withValues)
                return cqlTool.FillWithValues(sb.ToString(), out values);
            else
            {
                values = null;
                return cqlTool.FillWithEncoded(sb.ToString());
            }
        }

        public string GetUpdate(out object[] values, int? ttl, DateTimeOffset? timestamp, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("UPDATE ");
            sb.Append(QuotedTableName);
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

            foreach (KeyValuePair<string, Tuple<string, object, int>> mapping in Mappings)
            {
                object o = mapping.Value.Item2;
                if (o != null)
                {
                    var val = (object) null;
                    MemberInfo propsOrField = o.GetType().GetPropertiesOrFields().SingleOrDefault(pf => pf.Name == mapping.Value.Item1);

                    if (o.GetType().IsPrimitive || propsOrField == null)
                        val = o;
                    else
                        val = propsOrField.GetValueFromPropertyOrField(o);

                    if (!Alter.ContainsKey(mapping.Key))
                        throw new CqlArgumentException("Unknown column: " + mapping.Key);
                    setStatements.Add(Alter[mapping.Key].QuoteIdentifier() + " = " + cqlTool.AddValue(val));
                }
                else
                {
                    if (!Alter.ContainsKey(mapping.Key))
                        throw new CqlArgumentException("Unknown column: " + mapping.Key);
                    setStatements.Add(Alter[mapping.Key].QuoteIdentifier() + " = NULL");
                }
            }

            if (setStatements.Count == 0)
                throw new CqlArgumentException("Nothing to update");
            sb.Append(String.Join(", ", setStatements));

            if (WhereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(WhereClause);
            }

            if (UpdateIfClause.Length > 0)
            {
                sb.Append(" IF ");
                sb.Append(UpdateIfClause);
            }

            if (withValues)
                return cqlTool.FillWithValues(sb.ToString(), out values);
            values = null;
            return cqlTool.FillWithEncoded(sb.ToString());
        }

        public string GetCount(out object[] values, bool withValues = true)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT count(*) FROM ");
            sb.Append(QuotedTableName);

            if (WhereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(WhereClause);
            }

            if (Limit > 0)
            {
                sb.Append(" LIMIT ");
                sb.Append(Limit);
            }

            if (withValues)
                return cqlTool.FillWithValues(sb.ToString(), out values);
            values = null;
            return cqlTool.FillWithEncoded(sb.ToString());
        }

        public void Evaluate(Expression expression)
        {
            Visit(expression);
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                foreach (MemberBinding binding in node.Bindings)
                {
                    if (binding is MemberAssignment)
                    {
                        using (currentBindingName.set(binding.Member.Name))
                            Visit((binding as MemberAssignment).Expression);
                    }
                }
                return node;
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            if (phasePhase.get() == ParsePhase.What)
            {
                using (phasePhase.set(ParsePhase.SelectBinding))
                using (currentBindingName.set(node.Parameters[0].Name))
                    Visit(node.Body);
                return node;
            }
            return base.VisitLambda(node);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                if (node.Members != null)
                {
                    for (int i = 0; i < node.Members.Count; i++)
                    {
                        Expression binding = node.Arguments[i];
                        if (binding.NodeType == ExpressionType.Parameter)
                            throw new CqlLinqNotSupportedException(binding, phasePhase.get());

                        using (currentBindingName.set(node.Members[i].Name))
                            Visit(binding);
                    }
                }
                return node;
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Select")
            {
                Visit(node.Arguments[0]);

                using (phasePhase.set(ParsePhase.What))
                    Visit(node.Arguments[1]);

                return node;
            }
            if (node.Method.Name == "Where")
            {
                Visit(node.Arguments[0]);

                using (phasePhase.set(ParsePhase.Condition))
                {
                    if (WhereClause.Length != 0)
                        WhereClause.Append(" AND ");
                    Visit(node.Arguments[1]);
                }
                return node;
            }
            if (node.Method.Name == "UpdateIf" || node.Method.Name == "DeleteIf")
            {
                this.Visit(node.Arguments[0]);

                using (phasePhase.set(ParsePhase.Condition))
                {
                    if (UpdateIfClause.Length != 0)
                        UpdateIfClause.Append(" AND ");
                    using (currentConditionBuilder.set(UpdateIfClause))
                        this.Visit(node.Arguments[1]);
                }
                return node;
            }
            if (node.Method.Name == "Take")
            {
                Visit(node.Arguments[0]);
                using (phasePhase.set(ParsePhase.Take))
                    Visit(node.Arguments[1]);
                return node;
            }
            if (node.Method.Name == "OrderBy" || node.Method.Name == "ThenBy")
            {
                Visit(node.Arguments[0]);
                using (phasePhase.set(ParsePhase.OrderBy))
                    Visit(node.Arguments[1]);
                return node;
            }
            if (node.Method.Name == "OrderByDescending" || node.Method.Name == "ThenByDescending")
            {
                Visit(node.Arguments[0]);
                using (phasePhase.set(ParsePhase.OrderByDescending))
                    Visit(node.Arguments[1]);
                return node;
            }
            if (node.Method.Name == "FirstOrDefault" || node.Method.Name == "First")
            {
                Visit(node.Arguments[0]);
                if (node.Arguments.Count == 3)
                {
                    using (phasePhase.set(ParsePhase.Condition))
                        Visit(node.Arguments[2]);
                }
                Limit = 1;
                return node;
            }

            if (phasePhase.get() == ParsePhase.Condition)
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
                    currentConditionBuilder.get().Append(" IN (");
                    var values = (IEnumerable) Expression.Lambda(inp).Compile().DynamicInvoke();
                    bool first = false;
                    foreach (object obj in values)
                    {
                        if (!first)
                            first = true;
                        else
                            currentConditionBuilder.get().Append(", ");
                        currentConditionBuilder.get().Append(cqlTool.AddValue(obj));
                    }
                    if (!first)
                        throw new CqlArgumentException("Collection " + inp + " is empty.");
                    currentConditionBuilder.get().Append(")");
                    return node;
                }
                if (node.Method.Name == "CompareTo")
                {
                    Visit(node.Object);
                    currentConditionBuilder.get().Append(" " + binaryExpressionTag.get() + " ");
                    Visit(node.Arguments[0]);
                    return node;
                }
                if (node.Method.Name == "Equals")
                {
                    Visit(node.Object);
                    currentConditionBuilder.get().Append(" = ");
                    Visit(node.Arguments[0]);
                    return node;
                }
                if (node.Type.Name == "CqlToken")
                {
                    currentConditionBuilder.get().Append("token(");
                    ReadOnlyCollection<Expression> exprs = node.Arguments;
                    Visit(exprs.First());
                    foreach (Expression e in exprs.Skip(1))
                    {
                        currentConditionBuilder.get().Append(", ");
                        Visit(e);
                    }
                    currentConditionBuilder.get().Append(")");
                    return node;
                }
                object val = Expression.Lambda(node).Compile().DynamicInvoke();
                currentConditionBuilder.get().Append(cqlTool.AddValue(val));
                return node;
            }

            throw new CqlLinqNotSupportedException(node, phasePhase.get());
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
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (CQLTags.ContainsKey(node.NodeType))
                {
                    currentConditionBuilder.get().Append(CQLTags[node.NodeType] + " (");
                    Visit(DropNullableConversion(node.Operand));
                    currentConditionBuilder.get().Append(")");
                }
                else
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    currentConditionBuilder.get().Append(cqlTool.AddValue(val));
                }
                return node;
            }
            if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                if (node.NodeType == ExpressionType.Convert && node.Type.Name == "Nullable`1")
                {
                    return Visit(node.Operand);
                }
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
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
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (CQLTags.ContainsKey(node.NodeType))
                {
                    if (IsCompareTo(node.Left))
                    {
                        if (IsZero(node.Right))
                        {
                            using (binaryExpressionTag.set(CQLTags[node.NodeType]))
                                Visit(node.Left);
                            return node;
                        }
                    }
                    else if (IsCompareTo(node.Right))
                    {
                        if (IsZero(node.Left))
                        {
                            using (binaryExpressionTag.set(CQLTags[CQLInvTags[node.NodeType]]))
                                Visit(node.Right);
                            return node;
                        }
                    }
                    else
                    {
                        Visit(DropNullableConversion(node.Left));
                        currentConditionBuilder.get().Append(" " + CQLTags[node.NodeType] + " ");
                        Visit(DropNullableConversion(node.Right));
                        return node;
                    }
                }
                else if (!CQLUnsupTags.Contains(node.NodeType))
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    currentConditionBuilder.get().Append(cqlTool.AddValue(val));
                    return node;
                }
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                object val = Expression.Lambda(node).Compile().DynamicInvoke();
                if (Alter.ContainsKey(currentBindingName.get()))
                {
                    Mappings[currentBindingName.get()] = Tuple.Create(currentBindingName.get(), val, Mappings.Count);
                    SelectFields.Add(currentBindingName.get());
                }
                else
                {
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object, int>(null, val, Mappings.Count);
                }
                return node;
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is ITable)
            {
                var table = (node.Value as ITable);
                QuotedTableName = table.GetQuotedTableName();
                AllowFiltering = table.GetEntityType().GetCustomAttributes(typeof (AllowFilteringAttribute), false).Any();

                List<MemberInfo> props = table.GetEntityType().GetPropertiesOrFields();
                foreach (MemberInfo prop in props)
                {
                    string memName = CqlQueryTools.CalculateMemberName(prop);
                    Alter[prop.Name] = memName;
                }
                return node;
            }
            if (phasePhase.get() == ParsePhase.Condition)
            {
                currentConditionBuilder.get().Append(cqlTool.AddValue(node.Value));
                return node;
            }
            if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                if (Alter.ContainsKey(currentBindingName.get()))
                {
                    Mappings[currentBindingName.get()] = Tuple.Create(currentBindingName.get(), node.Value, Mappings.Count);
                    SelectFields.Add(currentBindingName.get());
                }
                else
                {
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object, int>(null, node.Value, Mappings.Count);
                }
                return node;
            }
            if (phasePhase.get() == ParsePhase.Take)
            {
                Limit = (int) node.Value;
                return node;
            }
            if (phasePhase.get() == ParsePhase.OrderBy || phasePhase.get() == ParsePhase.OrderByDescending)
            {
                OrderBy.Add(Alter[(string) node.Value].QuoteIdentifier() + (phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));
                return node;
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (node.Expression == null)
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    currentConditionBuilder.get().Append(cqlTool.AddValue(val));
                    return node;
                }
                if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    currentConditionBuilder.get().Append(Alter[node.Member.Name].QuoteIdentifier());
                    return node;
                }
                if (node.Expression.NodeType == ExpressionType.Constant)
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    if (val is CqlToken)
                    {
                        currentConditionBuilder.get().Append("token(");
                        currentConditionBuilder.get().Append(cqlTool.AddValue((val as CqlToken).Values.First()));
                        foreach (object e in (val as CqlToken).Values.Skip(1))
                        {
                            currentConditionBuilder.get().Append(", ");
                            currentConditionBuilder.get().Append(cqlTool.AddValue(e));
                        }
                        currentConditionBuilder.get().Append(")");
                    }
                    else
                    {
                        currentConditionBuilder.get().Append(cqlTool.AddValue(val));
                    }
                    return node;
                }
                if (node.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    currentConditionBuilder.get().Append(cqlTool.AddValue(val));
                    return node;
                }
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                string name = node.Member.Name;
                if (node.Expression == null)
                {
                    object val = Expression.Lambda(node).Compile().DynamicInvoke();
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object, int>(null, val, Mappings.Count);
                    return node;
                }
                if (node.Expression.NodeType == ExpressionType.Constant || node.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    if (Alter.ContainsKey(currentBindingName.get()))
                    {
                        object val = Expression.Lambda(node.Expression).Compile().DynamicInvoke();
                        Mappings[currentBindingName.get()] = Tuple.Create(name, val, Mappings.Count);
                        SelectFields.Add(name);
                    }
                    else
                    {
                        object val = Expression.Lambda(node).Compile().DynamicInvoke();
                        Mappings[currentBindingName.get()] = Tuple.Create<string, object, int>(null, val, Mappings.Count);
                    }
                    return node;
                }
                if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object, int>(name, name, Mappings.Count);
                    SelectFields.Add(name);
                    return node;
                }
            }
            else if (phasePhase.get() == ParsePhase.OrderBy || phasePhase.get() == ParsePhase.OrderByDescending)
            {
                string name = node.Member.Name;
                OrderBy.Add(Alter[name].QuoteIdentifier() + (phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));

                if ((node.Expression is ConstantExpression))
                {
                    return node;
                }
                if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    return node;
                }
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }
    }
}
