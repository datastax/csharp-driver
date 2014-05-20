//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents a Linq query that gets evaluated as a CQL statement.
    /// </summary>
    public class CqlQuery<TEntity> : CqlQueryBase<TEntity>, IQueryable, IQueryable<TEntity>, IOrderedQueryable
    {
        internal CqlQuery()
        {
            InternalInitialize(Expression.Constant(this), (Table<TEntity>) this);
        }

        internal CqlQuery(Expression expression, IQueryProvider table) : base(expression, table)
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IQueryProvider Provider
        {
            get { return GetTable() as IQueryProvider; }
        }

        public IEnumerator<TEntity> GetEnumerator()
        {
            throw new InvalidOperationException("Did you forget to Execute()?");
        }

        public new CqlQuery<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlQuery<TEntity> SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetSerialConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlQuery<TEntity> SetPageSize(int pageSize)
        {
            base.SetPageSize(pageSize);
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            return visitor.GetSelect(out values);
        }

        public override string ToString()
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            object[] _;
            return visitor.GetSelect(out _, false);
        }

        /// <summary>
        /// Evaluates the Linq query, executes the cql statement and adapts the results.
        /// </summary>
        public IEnumerable<TEntity> Execute()
        {
            return EndExecute(BeginExecute(null, null));
        }

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;

            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            object[] values;
            string cql = visitor.GetSelect(out values, withValues);

            return InternalBeginExecute(cql, values, visitor.Mappings, visitor.Alter, callback, state);
        }

        public IEnumerable<TEntity> EndExecute(IAsyncResult ar)
        {
            var outp = InternalEndExecute(ar);
            QueryTrace = outp.Info.QueryTrace;

            CqlColumn[] cols = outp.Columns;
            var colToIdx = new Dictionary<string, int>();
            for (int idx = 0; idx < cols.Length; idx++)
                colToIdx.Add(cols[idx].Name, idx);
            IEnumerable<Row> rows = outp.GetRows();
            var tag = (CqlQueryTag) Session.GetTag(ar);
            foreach (Row row in rows)
            {
                yield return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, tag.Mappings, tag.Alter);
            }
        }
    }
}