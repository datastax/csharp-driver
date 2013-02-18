using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Collections;

namespace Cassandra.Data.Linq
{

    public class CqlExpressionVisitor : ExpressionVisitor
    {
        public StringBuilder whereClause = new StringBuilder();
        public string tableName;

        public Dictionary<string, object> MappingVals = new Dictionary<string, object>();
        public Dictionary<string, string> MappingNames = new Dictionary<string, string>();
        public HashSet<string> SelectFields = new HashSet<string>();
        public List<string> OrderBy = new List<string>();

        public int Limit = 0;
        public bool AllowFiltering = false;

        enum ParsePhase { None, Select, What, Condition, SelectBinding,Take, First, OrderBy, OrderByDescending };

        VisitingParam<ParsePhase> phasePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        VisitingParam<string> currentBindingName = new VisitingParam<string>(null);


        public string GetSelect()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ");
            sb.Append(SelectFields.Count == 0?"* ":string.Join(",", from f in SelectFields select f.CqlIdentifier()));

            sb.Append(" FROM ");
            sb.Append(tableName.CqlIdentifier());

            if (whereClause.Length>0)
            {
                sb.Append(" WHERE ");
                sb.Append(whereClause);
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
            sb.Append(tableName.CqlIdentifier());

            if (whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(whereClause);
            }

            return sb.ToString();
        }

        public string GetUpdate()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE ");
            sb.Append(tableName.CqlIdentifier());
            sb.Append(" SET ");
            foreach (var al in MappingNames)
            {
                if (MappingVals.ContainsKey(al.Key))
                {
                    var o = MappingVals[al.Key];
                    if (o.GetType().IsPrimitive)
                    {
                        sb.Append(al.Key.CqlIdentifier() + "=" + o.Encode());
                    }
                    else
                    {
                        var val = o.GetType().GetField(al.Value).GetValue(o);
                        sb.Append(al.Key.CqlIdentifier() + "=" + val.Encode());
                    }
                }
            }

            if (whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(whereClause);
            }

            return sb.ToString();
        }

        public string GetCount()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT count(*) FROM ");
            sb.Append(tableName.CqlIdentifier());

            if (whereClause.Length > 0)
            {
                sb.Append(" WHERE ");
                sb.Append(whereClause);
            }

            if (Limit > 0)
            {
                sb.Append(" LIMIT ");
                sb.Append(Limit);
            }

            return sb.ToString();
        }


        StringBuilder sb = new StringBuilder();

        public void Evaluate(Expression expression)
        {
            this.Visit(expression);
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if (phasePhase.get() == ParsePhase.What)
            {
                foreach (var binding in node.Bindings)
                {
                    if (binding is MemberAssignment)
                    {
                        using (phasePhase.set(ParsePhase.SelectBinding))
                        using (currentBindingName.set(binding.Member.Name))
                            this.Visit((binding as MemberAssignment).Expression);
                    }
                }
            }
            return node;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            if (phasePhase.get() == ParsePhase.What)
            {
                for (int i = 0; i < node.Members.Count; i++)
                {
                    var binding = node.Arguments[i];
                    using (phasePhase.set(ParsePhase.SelectBinding))
                    using (currentBindingName.set(node.Members[i].Name))
                        this.Visit(binding);
                }
            }
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.Name == "Select")
            {
                this.Visit(m.Arguments[0]);

                using(phasePhase.set(ParsePhase.What))
                    this.Visit(m.Arguments[1]);

                return m;
            }
            else if (m.Method.Name == "Where")
            {
                this.Visit(m.Arguments[0]);

                using (phasePhase.set(ParsePhase.Condition))
                    this.Visit(m.Arguments[1]);
                
                return m;
            }
            else if (m.Method.Name == "Take")
            {
                this.Visit(m.Arguments[0]);
                using (phasePhase.set(ParsePhase.Take))
                    this.Visit(m.Arguments[1]);
                return m;
            }
            else if (m.Method.Name == "OrderBy" || m.Method.Name == "ThenBy")
            {
                this.Visit(m.Arguments[0]);
                using (phasePhase.set(ParsePhase.OrderBy))
                    this.Visit(m.Arguments[1]);
                return m;
            }
            else if (m.Method.Name == "OrderByDescending" || m.Method.Name == "ThenByDescending")
            {
                this.Visit(m.Arguments[0]);
                using (phasePhase.set(ParsePhase.OrderByDescending))
                    this.Visit(m.Arguments[1]);
                return m;
            }
            else if (m.Method.Name == "FirstOrDefault" || m.Method.Name == "First")
            {
                using (phasePhase.set(ParsePhase.First))
                    this.Visit(m.Arguments[0]);
                Limit = 1;
                return m;
            }

            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (m.Method.Name == "Contains")
                {
                    this.Visit(m.Arguments[0]);
                    whereClause.Append(" IN (");
                    var values = (IEnumerable)Expression.Lambda(m.Object).Compile().DynamicInvoke();
                    bool first = false;
                    foreach (var obj in values)
                    {
                        if (!first)
                            first = true;
                        else
                            whereClause.Append(", ");
                        whereClause.Append(obj.Encode());
                    }
                    whereClause.Append(")");
                    return m;
                }
            }

            throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
        }

        Dictionary<ExpressionType, string> CQLTags = new Dictionary<ExpressionType, string>()
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

        protected override Expression VisitUnary(UnaryExpression u)
        {
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (CQLTags.ContainsKey(u.NodeType))
                {
                    whereClause.Append(CQLTags[u.NodeType] + " (");
                    this.Visit(u.Operand);
                    whereClause.Append(")");
                }
            }
            return u;
        }
        
        protected override Expression VisitBinary(BinaryExpression b)
        {
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (CQLTags.ContainsKey(b.NodeType))
                {
                    this.Visit(b.Left);
                    whereClause.Append(" " + CQLTags[b.NodeType] + " ");
                    this.Visit(b.Right);
                }
            }
            return b;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (c.Value is ITable)
            {
                tableName = (c.Value as ITable).GetTableName();
                AllowFiltering = (c.Value as ITable).GetEntityType().GetCustomAttributes(typeof(AllowFilteringAttribute), false).Any();
            }
            else if (phasePhase.get() == ParsePhase.Condition)
            {
                whereClause.Append(c.Value.Encode());
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                MappingVals.Add(currentBindingName.get(), c.Value);
                MappingNames.Add(currentBindingName.get(), currentBindingName.get());
                SelectFields.Add(currentBindingName.get());
            }
            else if (phasePhase.get() == ParsePhase.Take)
            {
                Limit = (int)c.Value;
            }
            else if (phasePhase.get() == ParsePhase.OrderBy)
            {
                OrderBy.Add((string)c.Value + " ASC");
            }
            else if (phasePhase.get() == ParsePhase.OrderByDescending)
            {
                OrderBy.Add((string)c.Value + " DESC");
            }
            return c;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            if (phasePhase.get() == ParsePhase.Condition)
            {
                if (m.Expression.NodeType == ExpressionType.Parameter)
                {
                    whereClause.Append(m.Member.Name.CqlIdentifier());
                    return m;
                }
                else if (m.Expression.NodeType == ExpressionType.Constant)
                {
                    var val = Expression.Lambda(m).Compile().DynamicInvoke();
                    whereClause.Append(val.Encode());
                    return m;
                }
            }
            else if (phasePhase.get() == ParsePhase.SelectBinding)
            {
                var name = m.Member.Name;
                if ((m.Expression is ConstantExpression))
                {
                    var val = Expression.Lambda(m.Expression).Compile().DynamicInvoke();
                    MappingVals.Add(currentBindingName.get(), val);
                    MappingNames.Add(name, currentBindingName.get());
                    SelectFields.Add(name);
                    return m;
                }
                else if (m.Expression.NodeType == ExpressionType.Parameter)
                {
                    MappingVals.Add(currentBindingName.get(), name);
                    MappingNames.Add(name, currentBindingName.get());
                    SelectFields.Add(name);
                    return m;
                }
            }
            throw new NotSupportedException(string.Format("The member '{0}' is not supported", m.Member.Name));
        }
    }
}


