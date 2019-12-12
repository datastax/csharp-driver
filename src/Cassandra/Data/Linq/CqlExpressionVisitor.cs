//
//      Copyright (C) DataStax Inc.
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
using Cassandra.Data.Linq.ExpressionParsing;
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
        /// The initial capacity for WHERE and SET list parameters
        /// </summary>
        private const int DefaultClauseParameterCapacity = 8;

        private static readonly string Utf8MaxValue = Encoding.UTF8.GetString(new byte[] { 0xF4, 0x8F, 0xBF, 0xBF });

        private static readonly HashSet<ExpressionType> CqlUnsupTags = new HashSet<ExpressionType>
        {
            ExpressionType.Or,
            ExpressionType.OrElse
        };

        private readonly VisitingParam<string> _currentBindingName = new VisitingParam<string>();

        /// <summary>
        /// The active conditions (WHERE or IF clause)
        /// </summary>
        private IList<IConditionItem> _conditions;
        private readonly VisitingParam<ParsePhase> _parsePhase = new VisitingParam<ParsePhase>(ParsePhase.None);
        private readonly PocoData _pocoData;
        private bool _allowFiltering;
        private int _limit;

        private static readonly ICqlIdentifierHelper CqlIdentifierHelper = new CqlIdentifierHelper();

        private readonly IList<Tuple<PocoColumn, object, ExpressionType>> _projections =
            new List<Tuple<PocoColumn, object, ExpressionType>>();
        private readonly IList<Tuple<string, bool>> _orderBy = new List<Tuple<string, bool>>();
        private readonly IList<string> _groupBy = new List<string>();
        private readonly IList<string> _selectFields = new List<string>(CqlExpressionVisitor.DefaultClauseParameterCapacity);
        private readonly IList<IConditionItem> _where = new List<IConditionItem>(CqlExpressionVisitor.DefaultClauseParameterCapacity);
        private readonly IList<IConditionItem> _ifClause = new List<IConditionItem>(CqlExpressionVisitor.DefaultClauseParameterCapacity);

        private readonly string _tableName;
        private readonly string _keyspaceName;
        private bool _isSelectQuery;

        public CqlExpressionVisitor(PocoData pocoData, string tableName, string keyspaceName)
        {
            _pocoData = pocoData;
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _keyspaceName = keyspaceName;
            _conditions = _where;
        }

        /// <summary>
        /// Gets a cql SELECT statement based on the current state
        /// </summary>
        public string GetSelect(Expression expression, out object[] values)
        {
            _isSelectQuery = true;
            Visit(expression);
            var query = new StringBuilder(CqlExpressionVisitor.DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("SELECT ");
            query.Append(_selectFields.Count == 0
                ? _pocoData.Columns
                           .Select(c => CqlExpressionVisitor.CqlIdentifierHelper.EscapeIdentifierIfNecessary(_pocoData, c.ColumnName))
                           .ToCommaDelimitedString()
                : _selectFields.Select(c => CqlExpressionVisitor.CqlIdentifierHelper.EscapeIdentifierIfNecessary(_pocoData, c))
                               .ToCommaDelimitedString());

            query.Append(" FROM ");
            query.Append(CqlExpressionVisitor.CqlIdentifierHelper.EscapeTableNameIfNecessary(_pocoData, _keyspaceName, _tableName));

            GenerateConditions(_where, "WHERE", query, parameters, expression);

            if (_groupBy.Count > 0)
            {
                query.Append(" GROUP BY ");
                query.Append(string.Join(", ", _groupBy));
            }

            if (_orderBy.Count > 0)
            {
                query.Append(" ORDER BY ");
                query.Append(string.Join(
                    ", ", 
                    _orderBy.Select(item => 
                        CqlExpressionVisitor.CqlIdentifierHelper.EscapeIdentifierIfNecessary(_pocoData, item.Item1) 
                        + (item.Item2 ? "" : " DESC"))));
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

        private void GenerateConditions(IList<IConditionItem> conditions, string clause, StringBuilder query, IList<object> parameters,
                                        Expression expression)
        {
            if (conditions.Count <= 0)
            {
                return;
            }

            query.Append(" ");
            query.Append(clause);
            query.Append(" ");

            try
            {
                for (var i = 0; i < conditions.Count; i++)
                {
                    var condition = conditions[i];
                    if (i > 0)
                    {
                        query.Append(" AND ");
                    }

                    condition.ToCql(_pocoData, query, parameters);
                }
            }
            catch (Exception ex) when (ex is ArgumentException || ex is InvalidOperationException)
            {
                throw new CqlLinqNotSupportedException(expression, ParsePhase.Condition);
            }
        }

        /// <summary>
        /// Gets a cql DELETE statement based on the current state
        /// </summary>
        public string GetDelete(Expression expression, out object[] values, DateTimeOffset? timestamp, bool ifExists)
        {
            Visit(expression);
            var query = new StringBuilder(CqlExpressionVisitor.DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("DELETE FROM ");
            query.Append(CqlExpressionVisitor.CqlIdentifierHelper.EscapeTableNameIfNecessary(_pocoData, _keyspaceName, _tableName));
            if (timestamp != null)
            {
                query.Append(" USING TIMESTAMP ?");
                parameters.Add((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
            }

            GenerateConditions(_where, "WHERE", query, parameters, expression);

            if (ifExists)
            {
                query.Append(" IF EXISTS");
            }

            if (_ifClause.Count > 0 && ifExists)
            {
                throw new CqlArgumentException("IF EXISTS and IF (condition) are mutually exclusive");
            }

            GenerateConditions(_ifClause, "IF", query, parameters, expression);

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
            var query = new StringBuilder(CqlExpressionVisitor.DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("UPDATE ");
            query.Append(CqlExpressionVisitor.CqlIdentifierHelper.EscapeTableNameIfNecessary(_pocoData, _keyspaceName, _tableName));
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
                var columnName = CqlExpressionVisitor.CqlIdentifierHelper.EscapeIdentifierIfNecessary(_pocoData, column.ColumnName);
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
            query.Append(string.Join(", ", setStatements));

            GenerateConditions(_where, "WHERE", query, parameters, expression);

            GenerateConditions(_ifClause, "IF", query, parameters, expression);

            values = parameters.ToArray();
            return query.ToString();
        }

        public string GetCount(Expression expression, out object[] values)
        {
            Visit(expression);
            var query = new StringBuilder(CqlExpressionVisitor.DefaultQueryStringCapacity);
            var parameters = new List<object>();
            query.Append("SELECT count(*) FROM ");
            query.Append(CqlExpressionVisitor.CqlIdentifierHelper.EscapeTableNameIfNecessary(_pocoData, _keyspaceName, _tableName));

            GenerateConditions(_where, "WHERE", query, parameters, expression);

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
            if (phase == ParsePhase.Condition && Utils.IsTuple(node.Type))
            {
                EvaluateCompositeColumn(node);
                return node;
            }
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
            if (node.Method.DeclaringType == typeof(CqlMthHelps) || node.Method.DeclaringType == typeof(Enumerable))
            {
                switch (node.Method.Name)
                {
                    case nameof(CqlMthHelps.Select):
                        Visit(node.Arguments[0]);
                        using (_parsePhase.Set(ParsePhase.Select))
                        {
                            Visit(node.Arguments[1]);
                        }
                        return node;

                    case nameof(CqlMthHelps.Where):
                        Visit(node.Arguments[0]);
                        using (_parsePhase.Set(ParsePhase.Condition))
                        {
                            _where.Add(new BinaryConditionItem());
                            Visit(node.Arguments[1]);
                        }
                        return node;

                    case nameof(CqlMthHelps.UpdateIf):
                    case nameof(CqlMthHelps.DeleteIf):
                        Visit(node.Arguments[0]);
                        using (_parsePhase.Set(ParsePhase.Condition))
                        {
                            _ifClause.Add(new BinaryConditionItem());
                            _conditions = _ifClause;
                            Visit(node.Arguments[1]);
                            _conditions = _where;
                        }
                        return node;

                    case nameof(CqlMthHelps.UpdateIfExists):
                    case nameof(CqlMthHelps.UpdateIfNotExists):
                        _ifClause.Add(new ExistsConditionItem(node.Method.Name == nameof(CqlMthHelps.UpdateIfExists)));
                        Visit(node.Arguments[0]);
                        return node;

                    case nameof(CqlMthHelps.Take):
                        Visit(node.Arguments[0]);
                        using (_parsePhase.Set(ParsePhase.Take))
                        {
                            Visit(node.Arguments[1]);
                        }
                        return node;

                    case nameof(CqlMthHelps.GroupBy):
                        Visit(node.Arguments[0]);
                        using (_parsePhase.Set(ParsePhase.GroupBy))
                        {
                            Visit(node.Arguments[1]);
                        }
                        return node;

                    case nameof(CqlMthHelps.OrderBy):
                    case nameof(CqlMthHelps.ThenBy):
                        Visit(node.Arguments[0]);
                        using (_parsePhase.Set(ParsePhase.OrderBy))
                        {
                            Visit(node.Arguments[1]);
                        }
                        return node;

                    case nameof(CqlMthHelps.OrderByDescending):
                    case nameof(CqlMthHelps.ThenByDescending):
                        Visit(node.Arguments[0]);
                        using (_parsePhase.Set(ParsePhase.OrderByDescending))
                        {
                            Visit(node.Arguments[1]);
                        }
                        return node;

                    case nameof(CqlMthHelps.FirstOrDefault) when node.Method.DeclaringType == typeof(CqlMthHelps):
                    case nameof(CqlMthHelps.First) when node.Method.DeclaringType == typeof(CqlMthHelps):
                        Visit(node.Arguments[0]);
                        if (node.Arguments.Count == 3)
                        {
                            using (_parsePhase.Set(ParsePhase.Condition))
                            {
                                _where.Add(new BinaryConditionItem());
                                Visit(node.Arguments[2]);
                            }
                        }
                        _limit = 1;
                        return node;

                    case nameof(CqlMthHelps.AllowFiltering):
                        Visit(node.Arguments[0]);
                        _allowFiltering = true;
                        return node;

                    case nameof(Enumerable.Min):
                    case nameof(Enumerable.Max):
                    case nameof(Enumerable.Average):
                    case nameof(Enumerable.Sum):
                    case nameof(Enumerable.Count):
                        return FillAggregate(initialPhase, node);
                }
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
                value = CqlExpressionVisitor.GetClosureValue((MemberExpression) node);
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
            var condition = _conditions.Last();
            switch (node.Method.Name)
            {
                case nameof(Enumerable.Contains):
                    EvaluateContainsMethod(node);
                    return node;

                case nameof(Tuple.Create) when node.Method.DeclaringType == typeof(Tuple):
                    EvaluateCompositeColumn(node);
                    return node;

                case nameof(string.StartsWith) when node.Method.DeclaringType == typeof(string):
                    Visit(node.Object);
                    var startsWithArgument = node.Arguments[0];
                    var startString = (string)Expression.Lambda(startsWithArgument).Compile().DynamicInvoke();
                    var endString = startString + CqlExpressionVisitor.Utf8MaxValue;
                    // Create 2 conditions, ie: WHERE col1 >= startString AND col2 < endString
                    var column = condition.Column;
                    condition.SetOperator(ExpressionType.GreaterThanOrEqual)
                             .SetParameter(startString);
                    
                    condition = new BinaryConditionItem();
                    condition.SetColumn(column)
                             .SetOperator(ExpressionType.LessThan)
                             .SetParameter(endString);
                    _conditions.Add(condition);
                    return node;

                case nameof(IComparable.CompareTo):
                    // Allow comparison to zero
                    condition.SetAsCompareTo();
                    Visit(node.Object);
                    Visit(node.Arguments[0]);
                    return node;

                case nameof(object.Equals):
                    Visit(node.Object);
                    condition.SetOperator(ExpressionType.Equal);
                    Visit(node.Arguments[0]);
                    return node;

                case nameof(CqlToken.Create) when node.Method.DeclaringType == typeof(CqlToken):
                case nameof(CqlFunction.Token) when node.Method.DeclaringType == typeof(CqlFunction):
                    condition.SetFunctionName("token").AllowMultipleColumns().AllowMultipleParameters();
                    foreach (var argument in node.Arguments)
                    {
                        Visit(argument);
                    }
                    return node;

                case nameof(CqlFunction.MaxTimeUuid):
                case nameof(CqlFunction.MinTimeUuid):
                    condition.SetFunctionName(node.Method.Name.ToLowerInvariant());
                    Visit(node.Arguments[0]);
                    return node;
            }
            // Try to invoke to obtain the parameter value
            condition.SetParameter(Expression.Lambda(node).Compile().DynamicInvoke());
            return node;
        }

        private void EvaluateContainsMethod(MethodCallExpression node)
        {
            Expression columnExpression;
            Expression parameterExpression;
            if (node.Object == null)
            {
                columnExpression = node.Arguments[1];
                parameterExpression = node.Arguments[0];
            }
            else
            {
                columnExpression = node.Arguments[0];
                parameterExpression = node.Object;
            }
            if (columnExpression.NodeType != ExpressionType.Call && columnExpression.NodeType != ExpressionType.New)
            {
                // Use the expression visitor to extract the column
                Visit(columnExpression);
            }
            else
            {
                EvaluateCompositeColumn(columnExpression);
            }
            
            var values = Expression.Lambda(parameterExpression).Compile().DynamicInvoke() as IEnumerable;
            if (values == null)
            {
                throw new InvalidOperationException("Contains parameter must be IEnumerable");
            }
            if (values is string)
            {
                throw new InvalidOperationException("String.Contains() is not supported for CQL IN clause");
            }

            if (!(_conditions.Last() is BinaryConditionItem condition))
            {
                throw new CqlLinqNotSupportedException(node, _parsePhase.Get());
            }
            condition.SetInClause(values);
        }

        private void EvaluateCompositeColumn(Expression expression)
        {
            var condition = _conditions.Last();
            ICollection<Expression> columnsExpression;
            switch (expression.NodeType)
            {
                case ExpressionType.Call:
                    columnsExpression = ((MethodCallExpression) expression).Arguments;
                    break;
                case ExpressionType.New:
                    columnsExpression = ((NewExpression) expression).Arguments;
                    break;
                default:
                    throw new CqlLinqNotSupportedException(expression, _parsePhase.Get());
            }
            condition.AllowMultipleColumns();
            foreach (var c in columnsExpression)
            {
                Visit(c);
            }
        }

        private bool EvaluateOperatorMethod(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof (CqlOperator))
            {
                return false;
            }
            ExpressionType expressionType;
            switch (node.Method.Name)
            {
                case nameof(CqlOperator.Append):
                    expressionType = ExpressionType.AddAssign;
                    break;
                case nameof(CqlOperator.Prepend):
                    expressionType = ExpressionType.PreIncrementAssign;
                    break;
                case nameof(CqlOperator.SubstractAssign):
                    expressionType = ExpressionType.SubtractAssign;
                    break;
                default:
                    return false;
            }
            var column = _pocoData.GetColumnByMemberName(_currentBindingName.Get());
            if (expressionType == ExpressionType.SubtractAssign && node.Arguments.Count == 1 &&
                typeof(IDictionary).GetTypeInfo().IsAssignableFrom(column.ColumnType))
            {
                throw new InvalidOperationException("Use dedicated method to substract assign keys only for maps");
            }
            if (node.Arguments.Count < 1 || node.Arguments.Count > 2)
            {
                throw new InvalidOperationException(
                    "Only up to 2 arguments are supported for CqlOperator functions");
            }
            // Use the last argument (valid for maps and list/sets)
            var argument = node.Arguments[node.Arguments.Count - 1];
            var value = Expression.Lambda(argument).Compile().DynamicInvoke();
            _projections.Add(Tuple.Create(column, value, expressionType));
            return true;
        }

        private static Expression DropNullableConversion(Expression node)
        {
            if (node is UnaryExpression && node.NodeType == ExpressionType.Convert && node.Type.GetTypeInfo().IsGenericType &&
                string.Compare(node.Type.Name, "Nullable`1", StringComparison.Ordinal) == 0)
            {
                return (node as UnaryExpression).Operand;
            }
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (_parsePhase.Get() == ParsePhase.Condition)
            {
                var condition = _conditions.Last();
                if (node.NodeType == ExpressionType.Not && node.Operand.NodeType == ExpressionType.MemberAccess)
                {
                    // We are evaluating a boolean expression parameter, the value we are trying to match is false
                    FillBooleanCondition((MemberExpression) node.Operand, condition, false);
                    return node;
                }
                if (BinaryConditionItem.IsSupported(node.NodeType))
                {
                    condition.SetOperator(node.NodeType);
                    Visit(CqlExpressionVisitor.DropNullableConversion(node.Operand));
                }
                else if (node.NodeType == ExpressionType.Convert)
                {
                    Visit(node.Operand);
                }
                else
                {
                    var val = Expression.Lambda(node).Compile().DynamicInvoke();
                    condition.SetParameter(val);
                }
                return node;
            }
            if (_parsePhase.Get() == ParsePhase.SelectBinding)
            {
                if (node.NodeType == ExpressionType.Convert && node.Type.Name == "Nullable`1")
                {
                    // ReSharper disable once AssignNullToNotNullAttribute
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

        private void FillBooleanCondition(MemberExpression node, IConditionItem condition, bool? value = null)
        {
            condition
                .SetColumn(_pocoData.GetColumnByMemberName(node.Member.Name))
                .SetOperator(ExpressionType.Equal);

            if (value != null)
            {
                condition.SetParameter(value.Value);
            }
        }

        private bool IsBoolMember(Expression node)
        {
            return node.NodeType == ExpressionType.MemberAccess && node.Type == typeof(bool) &&
                   _pocoData.PocoType.GetTypeInfo().IsAssignableFrom(((MemberExpression) node).Member.DeclaringType);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (_parsePhase.Get() == ParsePhase.Condition)
            {
                if (node.NodeType == ExpressionType.AndAlso)
                {
                    // It's the AND of the WHERE/IF clause
                    Visit(node.Left);
                    
                    // Add the new condition for the right portion 
                    _conditions.Add(new BinaryConditionItem());
                    Visit(node.Right);
                    return node;
                }

                var condition = _conditions.Last();
                if (BinaryConditionItem.IsSupported(node.NodeType))
                {
                    if (node.NodeType == ExpressionType.Equal && IsBoolMember(node.Left))
                    {
                        // Handle x.prop == boolValue explicitly
                        FillBooleanCondition((MemberExpression)node.Left, condition);
                        Visit(node.Right);
                        return node;
                    }

                    if (node.NodeType == ExpressionType.Equal && IsBoolMember(node.Right))
                    {
                        // Handle boolValue == x.prop explicitly
                        FillBooleanCondition((MemberExpression)node.Right, condition);
                        Visit(node.Left);
                        return node;
                    }

                    Visit(CqlExpressionVisitor.DropNullableConversion(node.Left));
                    condition.SetOperator(node.NodeType);
                    Visit(CqlExpressionVisitor.DropNullableConversion(node.Right));
                    return node;
                }

                if (!CqlExpressionVisitor.CqlUnsupTags.Contains(node.NodeType))
                {
                    condition.SetParameter(Expression.Lambda(node).Compile().DynamicInvoke());
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
                    _conditions.Last().SetParameter(node.Value);
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
                    return FillCondition(node);
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

        private Expression FillCondition(MemberExpression node)
        {
            var condition = _conditions.Last();
            if (node.Expression == null || node.Expression.NodeType == ExpressionType.MemberAccess)
            {
                var val = CqlExpressionVisitor.GetClosureValue(node);
                condition.SetParameter(val);
                return node;
            }
            if (node.Expression.NodeType == ExpressionType.Parameter)
            {
                var column = _pocoData.GetColumnByMemberName(node.Member.Name);
                if (column == null)
                {
                    throw new InvalidOperationException($"No mapping defined for member: {node.Member.Name}");
                }

                condition.SetColumn(column);
                if (column.ColumnType == typeof(bool))
                {
                    condition.SetOperator(ExpressionType.Equal);
                    // We are evaluating a boolean expression parameter, the value we are trying to match is true 
                    condition.SetParameter(true);
                }
                return node;
            }
            if (node.Expression.NodeType == ExpressionType.Constant)
            {
                var val = CqlExpressionVisitor.GetClosureValue(node);
                if (val is CqlToken)
                {
                    var tokenValues = (val as CqlToken).Values;
                    condition.SetFunctionName("token").AllowMultipleParameters();
                    foreach (var pk in tokenValues)
                    {
                        condition.SetParameter(pk);
                    }
                }
                else
                {
                    condition.SetParameter(val);
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
                    var value = CqlExpressionVisitor.GetClosureValue(node);
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
                value = CqlExpressionVisitor.GetFieldValue(node);
            }
            else if (node.Member.MemberType == MemberTypes.Property)
            {
                value = CqlExpressionVisitor.GetPropertyValue(node);
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
                var instance = CqlExpressionVisitor.GetClosureValue((MemberExpression)node.Expression);
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

            if (node.Expression == null)
            {
                return propertyInfo.GetValue(null);
            }

            if (node.Expression is MemberExpression)
            {
                // Field property
                var instance = CqlExpressionVisitor.GetClosureValue((MemberExpression)node.Expression);
                return propertyInfo.GetValue(instance, null);
            }
            // Current instance property
            return propertyInfo.GetValue(((ConstantExpression)node.Expression).Value, null);
        }
    }
}
