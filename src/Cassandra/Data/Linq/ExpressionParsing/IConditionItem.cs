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

using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using Cassandra.Mapping;

namespace Cassandra.Data.Linq.ExpressionParsing
{
    /// <summary>
    /// Represents an individual condition part of the WHERE or IF clause.
    /// See CQL relation: http://cassandra.apache.org/doc/latest/cql/dml.html#grammar-token-relation 
    /// </summary>
    internal interface IConditionItem
    {
        PocoColumn Column { get; }

        /// <summary>
        /// Sets the operator of the binary condition
        /// </summary>
        IConditionItem SetOperator(ExpressionType expressionType);

        /// <summary>
        /// Sets the parameter or parameters of the condition
        /// </summary>
        IConditionItem SetParameter(object value);

        /// <summary>
        /// Sets the column or columns included in this condition.
        /// </summary>
        IConditionItem SetColumn(PocoColumn column);

        /// <summary>
        /// Determines if its possible to include multiple columns in this condition.
        /// For example: tuple relations (col1, col2) = ?.
        /// </summary>
        IConditionItem AllowMultipleColumns();

        /// <summary>
        /// Determines if its possible to include multiple parameters in this condition
        /// For example: token function calls token(col1, col2) >= token(?, ?).
        /// </summary>
        IConditionItem AllowMultipleParameters();

        /// <summary>
        /// Sets the CQL funcition of the current side of the condition.
        /// </summary>
        IConditionItem SetFunctionName(string name);

        /// <summary>
        /// Marks this condition as a result of a IComparable.CompareTo() call.
        /// </summary>
        IConditionItem SetAsCompareTo();

        /// <summary>
        /// Converts this instance into a query and parameters.
        /// </summary>
        void ToCql(PocoData pocoData, StringBuilder query, IList<object> parameters);
    }
}