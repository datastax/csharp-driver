// 
//       Copyright (C) 2018 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
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
        
        IConditionItem SetOperator(ExpressionType expressionType);

        IConditionItem SetParameter(object value);

        IConditionItem SetColumn(PocoColumn column);

        IConditionItem AllowMultipleColumns();

        IConditionItem AllowMultipleParameters();

        IConditionItem SetFunctionName(string name);

        void ToCql(PocoData pocoData, StringBuilder query, IList<object> parameters);

        void SetAsCompareTo();
    }
}