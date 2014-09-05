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

namespace Cassandra.Data.Linq
{
    public class CqlInsert<TEntity> : CqlCommand
    {
        private readonly TEntity _entity;
        private bool _ifNotExists;

        internal CqlInsert(TEntity entity, IQueryProvider table)
            : base(null, table)
        {
            _entity = entity;
        }

        public CqlInsert<TEntity> IfNotExists()
        {
            _ifNotExists = true;
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;
            return CqlQueryTools.GetInsertCQLAndValues(_entity, (GetTable()).GetQuotedTableName(), out values, _ttl, _timestamp, _ifNotExists,
                                                       withValues);
        }

        public override string ToString()
        {
            object[] _;
            return CqlQueryTools.GetInsertCQLAndValues(_entity, (GetTable()).GetQuotedTableName(), out _, _ttl, _timestamp, _ifNotExists, false);
        }
    }
}