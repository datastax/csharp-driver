using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public class CqlScalar<TEntity> : CqlQueryBase<TEntity>
    {
        internal CqlScalar(Expression expression, IQueryProvider table) : base(expression, table)
        {
        }

        public TEntity Execute()
        {
            return EndExecute(BeginExecute(null, null));
        }

        public new CqlScalar<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            return visitor.GetCount(out values);
        }

        public override string ToString()
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            object[] _;
            return visitor.GetCount(out _, false);
        }

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;

            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);

            object[] values;
            string cql = visitor.GetCount(out values, withValues);
            return InternalBeginExecute(cql, values, visitor.Mappings, visitor.Alter, callback, state);
        }

        public TEntity EndExecute(IAsyncResult ar)
        {
            var outp = InternalEndExecute(ar);
            QueryTrace = outp.Info.QueryTrace;

            CqlColumn[] cols = outp.Columns;
            if (cols.Length != 1)
                throw new InvalidOperationException("Single column is expected.");

            IEnumerable<Row> rows = outp.GetRows();
            bool first = false;
            TEntity ret = default(TEntity);
            foreach (Row row in rows)
            {
                if (first == false)
                {
                    ret = (TEntity) row[0];
                    first = true;
                }
                else
                    throw new InvalidOperationException("Single row is expected.");
            }
            if (!first)
                throw new InvalidOperationException("Single row is expected.");
            return ret;
        }
    }
}