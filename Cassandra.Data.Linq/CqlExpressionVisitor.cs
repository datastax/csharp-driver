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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Collections;

namespace Cassandra.Data.Linq
{
    internal enum ParsePhase { None, Select, What, Condition, SelectBinding, Take, OrderBy, OrderByDescending };

    public class CqlLinqNotSupportedException : NotSupportedException
    {
        public Expression Expression { get; private set; }
        internal CqlLinqNotSupportedException(Expression expression, ParsePhase parsePhase)
            : base(string.Format("The expression {0} = [{1}] is not supported in {2} parse phase.",
                        expression.NodeType.ToString(), expression.ToString(), parsePhase.ToString()))
        {
            Expression = expression;
        }
    }

    public class CqlArgumentException : ArgumentException
    {
        internal CqlArgumentException(string message)
            : base(message)
        { }
    }

    internal class CqlExpressionVisitor : ExpressionVisitor
    {
        public StringBuilder WhereClause = new StringBuilder();
        public string QuotedTableName;

        public Dictionary<string, string> Alter = new Dictionary<string, string>();
        public Dictionary<string, Tuple<string, object, int>> Mappings = new Dictionary<string, Tuple<string, object, int>>();
        public HashSet<string> SelectFields = new HashSet<string>();
        public List<string> OrderBy = new List<string>();

        public int Limit = 0;
        public bool AllowFiltering = false;

        VisitingParam<ParsePhase> phasePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        VisitingParam<string> currentBindingName = new VisitingParam<string>(null);
        VisitingParam<string> binaryExpressionTag = new VisitingParam<string>(null);


        public string GetSelect()
        {
            StringBuilder sb = new StringBuilder();
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
            
            return sb.ToString();
        }

        public string GetDelete(DateTimeOffset? timestamp)
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
            if (SelectFields.Count > 0)
                throw new CqlArgumentException("Unable to delete entity partially");
            return sb.ToString();
        }

        public string GetUpdate(int? ttl, DateTimeOffset? timestamp)
        {
            StringBuilder sb = new StringBuilder();
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

			foreach (var mapping in Mappings)
			{
				var o = mapping.Value.Item2;
				var val = (object)null;
                var propsOrField = o.GetType().GetPropertiesOrFields().SingleOrDefault(pf => pf.Name == mapping.Value.Item1);

				if (o.GetType().IsPrimitive || propsOrField == null)
				{
					val = o;
				}
				else
				{
					val = propsOrField.GetValueFromPropertyOrField(o);
				}

                if (!Alter.ContainsKey(mapping.Key))
                    throw new CqlArgumentException("Unknown column: " + mapping.Key);
				setStatements.Add(Alter[mapping.Key].QuoteIdentifier() + " = " + val.Encode());
			}

            if (setStatements.Count == 0)
                throw new CqlArgumentException("Nothing to update");
			sb.Append(String.Join(", ", setStatements));
	
            if (WhereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(WhereClause);
            }
            
            return sb.ToString();
        }

        public string GetCount()
        {
            StringBuilder sb = new StringBuilder();
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

            return sb.ToString();
        }

        public void Evaluate(Expression expression)
        {
            this.Visit(expression);
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                foreach (var binding in node.Bindings)
                {
                    if (binding is MemberAssignment)
                    {
                        using (currentBindingName.set(binding.Member.Name))
                            this.Visit((binding as MemberAssignment).Expression);
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
                    this.Visit(node.Body);
                return node;
            }
            return base.VisitLambda<T>(node);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                if (node.Members != null)
                {
                    for (int i = 0; i < node.Members.Count; i++)
                    {
                        var binding = node.Arguments[i];
                        if (binding.NodeType == ExpressionType.Parameter)
                            throw new CqlLinqNotSupportedException(binding, phasePhase.get());

                        using (currentBindingName.set(node.Members[i].Name))
                            this.Visit(binding);
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
                this.Visit(node.Arguments[0]);

                using(phasePhase.set(ParsePhase.What))
                    this.Visit(node.Arguments[1]);

                return node;
            }
            else if (node.Method.Name == "Where")
            {
                this.Visit(node.Arguments[0]);

                using (phasePhase.set(ParsePhase.Condition))
                {
                    if (WhereClause.Length != 0)
                        WhereClause.Append(" AND ");
                    this.Visit(node.Arguments[1]);
                }
                return node;
            }
            else if (node.Method.Name == "Take")
            {
                this.Visit(node.Arguments[0]);
                using (phasePhase.set(ParsePhase.Take))
                    this.Visit(node.Arguments[1]);
                return node;
            }
            else if (node.Method.Name == "OrderBy" || node.Method.Name == "ThenBy")
            {
                this.Visit(node.Arguments[0]);
                using (phasePhase.set(ParsePhase.OrderBy))
                    this.Visit(node.Arguments[1]);
                return node;
            }
            else if (node.Method.Name == "OrderByDescending" || node.Method.Name == "ThenByDescending")
            {
                this.Visit(node.Arguments[0]);
                using (phasePhase.set(ParsePhase.OrderByDescending))
                    this.Visit(node.Arguments[1]);
                return node;
            }
            else if (node.Method.Name == "FirstOrDefault" || node.Method.Name == "First")
            {
                this.Visit(node.Arguments[0]);
                if (node.Arguments.Count == 3)
                {
                    using (phasePhase.set(ParsePhase.Condition))
                        this.Visit(node.Arguments[2]);
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
                    this.Visit(what);
                    WhereClause.Append(" IN (");
                    var values = (IEnumerable)Expression.Lambda(inp).Compile().DynamicInvoke();
                    bool first = false;
                    foreach (var obj in values)
                    {
                        if (!first)
                            first = true;
                        else
                            WhereClause.Append(", ");
                        WhereClause.Append(obj.Encode());
                    }
                    if (!first)
                        throw new CqlArgumentException("Collection " + inp.ToString() + " is empty.");
                    WhereClause.Append(")");
                    return node;
                }
                else if (node.Method.Name == "CompareTo")
                {
                    this.Visit(node.Object);
                    WhereClause.Append(" " + binaryExpressionTag.get() + " ");
                    this.Visit(node.Arguments[0]);
                    return node;
                }
                else if(node.Method.Name == "Equals")
                {
                    this.Visit(node.Object);
                    WhereClause.Append(" = ");
                    this.Visit(node.Arguments[0]);
                    return node;
                }
                else if (node.Type.Name == "CqlToken")
                {
                    WhereClause.Append("token(");
                    var exprs = node.Arguments;
                    this.Visit(exprs.First());
                    foreach (var e in exprs.Skip(1))
                    {
                        WhereClause.Append(", ");
                        this.Visit(e);
                    }
                    WhereClause.Append(")");
                    return node;
                }
                else
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    WhereClause.Append(val.Encode());
                    return node;
                }

            }

            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }

        static readonly Dictionary<ExpressionType, string> CQLTags = new Dictionary<ExpressionType, string>()
        {
            {ExpressionType.Not,"NOT"},
            {ExpressionType.And,"AND"},
            {ExpressionType.AndAlso,"AND"},
			{ExpressionType.Equal,"="},
			{ExpressionType.NotEqual,"<>"},
			{ExpressionType.GreaterThan,">"},
			{ExpressionType.GreaterThanOrEqual,">="},
			{ExpressionType.LessThan,"<"},
			{ExpressionType.LessThanOrEqual,"<="}
        };

        static readonly Dictionary<ExpressionType, ExpressionType> CQLInvTags = new Dictionary<ExpressionType, ExpressionType>()
        {
			{ExpressionType.Equal,ExpressionType.Equal},
			{ExpressionType.NotEqual,ExpressionType.NotEqual},
			{ExpressionType.GreaterThan,ExpressionType.LessThan},
			{ExpressionType.GreaterThanOrEqual,ExpressionType.LessThanOrEqual},
			{ExpressionType.LessThan,ExpressionType.GreaterThan},
			{ExpressionType.LessThanOrEqual,ExpressionType.GreaterThanOrEqual}
        };
        
        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (CQLTags.ContainsKey(node.NodeType))
                {
                    WhereClause.Append(CQLTags[node.NodeType] + " (");
                    this.Visit(node.Operand);
                    WhereClause.Append(")");
                }
                else
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    WhereClause.Append(val.Encode());
                }
                return node;
            }
            if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                if (node.NodeType == ExpressionType.Convert && node.Type.Name == "Nullable`1")
                {
                    return this.Visit(node.Operand);
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
                    if (((int)(node as ConstantExpression).Value) == 0)
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
                                this.Visit(node.Left);
                            return node;
                        }
                    }
                    else if (IsCompareTo(node.Right))
                    {
                        if (IsZero(node.Left))
                        {
                            using (binaryExpressionTag.set(CQLTags[CQLInvTags[node.NodeType]]))
                                this.Visit(node.Right);
                            return node;
                        }
                    }
                    else
                    {
                        this.Visit(node.Left);
                        WhereClause.Append(" " + CQLTags[node.NodeType] + " ");
                        this.Visit(node.Right);
                        return node;
                    }
                }
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                var val = Expression.Lambda(node).Compile().DynamicInvoke();
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
                AllowFiltering = table.GetEntityType().GetCustomAttributes(typeof(AllowFilteringAttribute), false).Any();
                
                var props = table.GetEntityType().GetPropertiesOrFields();
                foreach (var prop in props)
                {
                    var memName = CqlQueryTools.CalculateMemberName(prop);
                    Alter[prop.Name] = memName;
                }
                return node;
            }
            else if (phasePhase.get() == ParsePhase.Condition)
            {
                WhereClause.Append(node.Value.Encode());
                return node;
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
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
            else if (phasePhase.get() == ParsePhase.Take)
            {
                Limit = (int)node.Value;
                return node;
            }
            else if (phasePhase.get() == ParsePhase.OrderBy || phasePhase.get() == ParsePhase.OrderByDescending)
            {
                OrderBy.Add(Alter[(string)node.Value].QuoteIdentifier() + (phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));
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
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    WhereClause.Append(val.Encode());
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    WhereClause.Append(Alter[node.Member.Name].QuoteIdentifier());
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.Constant)
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    if (val is CqlToken)
                    {
                        WhereClause.Append("token(");
                        WhereClause.Append((val as CqlToken).Values.First().Encode());
                        foreach (var e in (val as CqlToken).Values.Skip(1))
                        {
                            WhereClause.Append(", ");
                            WhereClause.Append(e.Encode());
                        }
                        WhereClause.Append(")");
                    }
                    else
                    {
                        WhereClause.Append(val.Encode());
                    }
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    WhereClause.Append(val.Encode());
                    return node;
                }
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                var name = node.Member.Name;
                if (node.Expression == null)
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object, int>(null, val, Mappings.Count);
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.Constant || node.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    if (Alter.ContainsKey(currentBindingName.get()))
                    {
                        var val = Expression.Lambda(node.Expression).Compile().DynamicInvoke();
                        Mappings[currentBindingName.get()] = Tuple.Create(name, val, Mappings.Count);
                        SelectFields.Add(name);
                    }
                    else
                    {
                        var val = Expression.Lambda(node).Compile().DynamicInvoke();
                        Mappings[currentBindingName.get()] = Tuple.Create<string,object,int>(null, val, Mappings.Count);
                    }
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object, int>(name, name, Mappings.Count);
                    SelectFields.Add(name);
                    return node;
                }
            }
            else if (phasePhase.get() == ParsePhase.OrderBy || phasePhase.get() == ParsePhase.OrderByDescending)
            {
                var name = node.Member.Name;
                OrderBy.Add(Alter[(string)name].QuoteIdentifier() + (phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));

                if ((node.Expression is ConstantExpression))
                {
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    return node;
                }
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }
    }
}


