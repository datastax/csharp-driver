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

using System.Linq.Expressions;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;

namespace Cassandra.Data.Linq
{
    public class CqlUpdate : CqlCommand
    {
        private readonly MapperFactory _mapperFactory;

        internal CqlUpdate(Expression expression, ITable table, StatementFactory stmtFactory, PocoData pocoData, MapperFactory mapperFactory)
            : base(expression, table, stmtFactory, pocoData)
        {
            _mapperFactory = mapperFactory;
        }

        protected internal override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            return visitor.GetUpdate(Expression, out values, _ttl, _timestamp, _mapperFactory);
        }

        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }
    }
}