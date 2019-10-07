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
using System.Text;
using Cassandra.Mapping;
using Cassandra.Mapping.Utils;

namespace Cassandra.Data.Linq.ExpressionParsing
{
    /// <summary>
    /// Represents a part of a WHERE clause
    /// </summary>
    internal class BinaryConditionItem : IConditionItem
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

        private static readonly Dictionary<ExpressionType, ExpressionType> InvertedOperations =
            new Dictionary<ExpressionType, ExpressionType>
            {
                {ExpressionType.Equal, ExpressionType.Equal},
                {ExpressionType.NotEqual, ExpressionType.NotEqual},
                {ExpressionType.GreaterThan, ExpressionType.LessThan},
                {ExpressionType.GreaterThanOrEqual, ExpressionType.LessThanOrEqual},
                {ExpressionType.LessThan, ExpressionType.GreaterThan},
                {ExpressionType.LessThanOrEqual, ExpressionType.GreaterThanOrEqual}
            };

        // Internally we use Index as a place holder for "IN" CQL operator
        private const ExpressionType InOperator = ExpressionType.Index;

        private static readonly ICqlIdentifierHelper CqlIdentifierHelper = new CqlIdentifierHelper();

        private readonly List<PocoColumn> _columns = new List<PocoColumn>(1);
        private readonly List<object> _parameters = new List<object>(1);
        private ExpressionType? _operator;
        private bool _allowMultipleColumns;
        private bool _allowMultipleParameters;
        private string _leftFunction;
        private string _rightFunction;
        private bool _isCompareTo;

        /// <summary>
        /// Yoda conditions are the ones the literal value of the condition comes first while the variable comes second.
        /// Ie: "? = col1"  
        /// </summary>
        private bool _isYoda;

        /// <summary>
        /// Returns the first column defined or null.
        /// </summary>
        public PocoColumn Column => _columns.Count >= 1 ? _columns[0] : null;

        public void SetInClause(IEnumerable values)
        {
            _operator = BinaryConditionItem.InOperator;
            _parameters.Add(values);
        }

        private string GetCqlOperator()
        {
            if (_operator == null)
            {
                throw new ArgumentException("Operator is not defined");
            }
            if (_operator == BinaryConditionItem.InOperator)
            {
                if (_isYoda)
                {
                    throw new InvalidOperationException(
                        "Inverted expressions are not supported when using IN operator");
                }
                return "IN";
            }

            var linqOperator = _operator.Value;
            if (_isYoda)
            {
                if (!BinaryConditionItem.InvertedOperations.TryGetValue(linqOperator, out var invertedOperator))
                {
                    throw new InvalidOperationException($"Operator {linqOperator} not supported");
                }

                linqOperator = invertedOperator;
            }

            if (!BinaryConditionItem.CqlTags.TryGetValue(linqOperator, out var cqlOperator))
            {
                throw new InvalidOperationException($"Operator {linqOperator} not supported");
            }
            return cqlOperator;
        }

        public IConditionItem SetOperator(ExpressionType expressionType)
        {
            _operator = expressionType;
            return this;
        }

        public IConditionItem SetParameter(object value)
        {
            if (_isCompareTo && _parameters.Count == 1)
            {
                if (Equals(value, 0))
                {
                    // CompareTo() expression its already parsed, zero is not the query parameter
                    return this;
                }
                throw new InvalidOperationException("CompareTo() Linq calls only supported to compare against 0");
            }
            if (!_allowMultipleParameters && _parameters.Count == 1)
            {
                throw new InvalidOperationException(
                    "Using multiple parameters on a single binary expression is not supported");
            }
            _parameters.Add(value);
            return this;
        }

        public IConditionItem SetColumn(PocoColumn column)
        {
            if (_parameters.Count > 0)
            {
                _isYoda = true;
            }

            if (!_allowMultipleColumns && _columns.Count == 1)
            {
                throw new InvalidOperationException("Multiple columns is not supported on a single binary expression");
            }

            _columns.Add(column);
            return this;
        }

        public IConditionItem AllowMultipleColumns()
        {
            _allowMultipleColumns = true;
            return this;
        }

        public IConditionItem AllowMultipleParameters()
        {
            _allowMultipleParameters = true;
            return this;
        }

        public IConditionItem SetFunctionName(string name)
        {
            if (_operator == null)
            {
                _leftFunction = name;
            }
            else
            {
                _rightFunction = name;
            }

            return this;
        }

        public void ToCql(PocoData pocoData, StringBuilder query, IList<object> parameters)
        {
            if (!_isYoda)
            {
                ToCqlColumns(pocoData, query, _leftFunction);

                query.Append(" ");
                query.Append(GetCqlOperator());
                query.Append(" ");

                ToCqlParameters(query, _rightFunction);
            }
            else
            {
                // Columns where defined after the operator
                // Use the right function if any
                ToCqlColumns(pocoData, query, _rightFunction);

                query.Append(" ");
                query.Append(GetCqlOperator());
                query.Append(" ");

                // Parameters where defined first
                // Use the left function if any
                ToCqlParameters(query, _leftFunction);
            }

            ChangeParameterType();

            foreach (var p in _parameters)
            {
                parameters.Add(p);
            }
        }

        private void ChangeParameterType()
        {
            var columnType = _columns[0].ColumnType;
            var p = _parameters[0];
            if (columnType != p?.GetType() && (columnType == typeof(short) || columnType == typeof(sbyte)) &&
                p is IConvertible)
            {
                // Constants for sbyte and short are compiled into Linq Expressions as integers  
                _parameters[0] = Convert.ChangeType(p, columnType);
            }
        }

        private void ToCqlColumns(PocoData pocoData, StringBuilder query, string functionName)
        {
            if (!_allowMultipleColumns)
            {
                if (functionName != null)
                {
                    query.Append(functionName);
                    query.Append("(");
                }

                query.Append(BinaryConditionItem.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, Column.ColumnName));

                if (functionName != null)
                {
                    query.Append(")");
                }
            }
            else
            {
                query.Append(functionName);
                query.Append("(");
                for (var i = 0; i < _columns.Count; i++)
                {
                    if (i > 0)
                    {
                        query.Append(", ");
                    }

                    query.Append(BinaryConditionItem.CqlIdentifierHelper.EscapeIdentifierIfNecessary(pocoData, _columns[i].ColumnName));
                }

                query.Append(")");
            }
        }

        private void ToCqlParameters(StringBuilder query, string functionName)
        {
            if (functionName != null)
            {
                query.Append(functionName);
                query.Append("(");
                // Multiple parameters on a single ConditionItem are only allowed within a function call
                query.Append(string.Join(", ", _parameters.Select(_ => "?")));
                query.Append(")");
            }
            else
            {
                query.Append("?");
            }
        }

        public IConditionItem SetAsCompareTo()
        {
            _isCompareTo = true;
            if (_parameters.Count == 1&& _operator != null)
            {
                if (!Equals(_parameters[0], 0))
                {
                    throw new InvalidOperationException("CompareTo() Linq calls only supported to compare against 0");
                }

                if (!BinaryConditionItem.InvertedOperations.TryGetValue(_operator.Value, out var invertedOperator))
                {
                    throw new InvalidOperationException($"Operator {_operator.Value} not supported");
                }

                _operator = invertedOperator;
            }
            _parameters.Clear();
            _columns.Clear();
            return this;
        }

        public static bool IsSupported(ExpressionType operatorType)
        {
            return BinaryConditionItem.CqlTags.ContainsKey(operatorType);
        }
    }
}