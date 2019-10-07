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
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using Cassandra.Mapping;

namespace Cassandra.Data.Linq.ExpressionParsing
{
    internal class ExistsConditionItem : IConditionItem
    {
        private readonly bool _positive;

        public PocoColumn Column =>
            throw new NotSupportedException("Getting column not supported on IF EXISTS condition");

        public ExistsConditionItem(bool positive)
        {
            _positive = positive;
        }

        public IConditionItem SetOperator(ExpressionType expressionType)
        {
            throw new NotSupportedException("Setting operator is not supported on IF EXISTS condition");
        }

        public IConditionItem SetParameter(object value)
        {
            throw new NotSupportedException("Setting parameter is not supported on IF EXISTS condition");
        }

        public IConditionItem SetColumn(PocoColumn column)
        {
            throw new NotSupportedException("Setting column is not supported on IF EXISTS condition");
        }

        public IConditionItem AllowMultipleColumns()
        {
            throw new NotSupportedException("Setting multiple columns is not supported on IF EXISTS condition");
        }

        public IConditionItem AllowMultipleParameters()
        {
            throw new NotSupportedException("Setting multiple parameters is not supported on IF EXISTS condition");
        }

        public IConditionItem SetFunctionName(string name)
        {
            throw new NotSupportedException("Setting function name is not supported on IF EXISTS condition");
        }

        public IConditionItem SetAsCompareTo()
        {
            throw new NotSupportedException("Setting function name is not supported on IF EXISTS condition");
        }

        public void ToCql(PocoData pocoData, StringBuilder query, IList<object> parameters)
        {
            query.Append(_positive ? "EXISTS" : "NOT EXISTS");
        }
    }
}