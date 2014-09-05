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

using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public class CqlUpdate : CqlCommand
    {
        internal CqlUpdate(Expression expression, IQueryProvider table)
            : base(expression, table)
        {
        }

        protected override string GetCql(out object[] values)
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            var type = GetTable().GetEntityType();
            return visitor.GetUpdate(out values, type, _ttl, _timestamp, withValues);
        }

        public override string ToString()
        {
            object[] _;
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            var type = GetTable().GetEntityType();
            return visitor.GetUpdate(out _, type, _ttl, _timestamp, false);
        }
    }
}