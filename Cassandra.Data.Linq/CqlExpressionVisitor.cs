using System;
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
            : base(string.Format("The expression '{0}' is not supported in '{1}' parse phase.",
                        expression.NodeType.ToString(), parsePhase.ToString()))
        {
            Expression = expression;
        }
    }

    internal class CqlExpressionVisitor : ExpressionVisitor
    {
        public StringBuilder WhereClause = new StringBuilder();
        public string TableName;

        public Dictionary<string, Tuple<string, object>> Mappings = new Dictionary<string, Tuple<string, object>>();
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
            sb.Append(SelectFields.Count == 0?"* ":string.Join(",", from f in SelectFields select f.CqlIdentifier()));

            sb.Append(" FROM ");
            sb.Append(TableName.CqlIdentifier());

            if (WhereClause.Length>0)
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
                sb.Append(" ALLOW FILTERING ");
            
            return sb.ToString();
        }

        public string GetDelete()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM ");
            sb.Append(TableName.CqlIdentifier());

            if (WhereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(WhereClause);
            }

            return sb.ToString();
        }

        public string GetUpdate()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE ");
            sb.Append(TableName.CqlIdentifier());
            sb.Append(" SET ");

			var setStatements = new List<string>();

			foreach (var mapping in Mappings)
			{
				var o = mapping.Value.Item2;
				var columnName = mapping.Key.CqlIdentifier();
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

				setStatements.Add(columnName + "=" + val.Encode());
			}

			sb.Append(String.Join(",", setStatements));
	
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
            sb.Append(TableName.CqlIdentifier());

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
                for (int i = 0; i < node.Members.Count; i++)
                {
                    var binding = node.Arguments[i];
                    using (currentBindingName.set(node.Members[i].Name))
                        this.Visit(binding);
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
                    this.Visit(node.Arguments[1]);
                
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
                else if (node.Method.Name == "CqlToken")
                {
                    WhereClause.Append("token (");
                    this.Visit(node.Arguments[0]);
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
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is ITable)
            {
                TableName = (node.Value as ITable).GetTableName();
                AllowFiltering = (node.Value as ITable).GetEntityType().GetCustomAttributes(typeof(AllowFilteringAttribute), false).Any();
                return node;
            }
            else if (phasePhase.get() == ParsePhase.Condition)
            {
                WhereClause.Append(node.Value.Encode());
                return node;
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                Mappings[currentBindingName.get()] = Tuple.Create<string, object>(currentBindingName.get(), node.Value);
                SelectFields.Add(currentBindingName.get());
                return node;
            }
            else if (phasePhase.get() == ParsePhase.Take)
            {
                Limit = (int)node.Value;
                return node;
            }
            else if (phasePhase.get() == ParsePhase.OrderBy || phasePhase.get() == ParsePhase.OrderByDescending)
            {
                OrderBy.Add(((string)node.Value).CqlIdentifier() + (phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));
                return node;
            }
            throw new CqlLinqNotSupportedException(node, phasePhase.get());
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    WhereClause.Append(node.Member.Name.CqlIdentifier());
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.Constant)
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    if (val is ICqlToken)
                    {
                        WhereClause.Append("token (");
                        WhereClause.Append((val as ICqlToken).Value.Encode());
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
                if (node.Expression.NodeType == ExpressionType.Constant || node.Expression.NodeType == ExpressionType.MemberAccess)
                {
                    var val = Expression.Lambda(node.Expression).Compile().DynamicInvoke();
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object>(name, val);
                    SelectFields.Add(name);
                    return node;
                }
                else if (node.Expression.NodeType == ExpressionType.Parameter)
                {
                    Mappings[currentBindingName.get()] = Tuple.Create<string, object>(name, name);
                    SelectFields.Add(name);
                    return node;
                }
            }
            else if (phasePhase.get() == ParsePhase.OrderBy || phasePhase.get() == ParsePhase.OrderByDescending)
            {
                var name = node.Member.Name;
                OrderBy.Add(name.CqlIdentifier() + (phasePhase.get() == ParsePhase.OrderBy ? " ASC" : " DESC"));
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


