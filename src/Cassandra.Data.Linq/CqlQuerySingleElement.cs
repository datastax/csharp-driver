using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public class CqlQuerySingleElement<TEntity> : CqlQueryBase<TEntity>
    {
        internal CqlQuerySingleElement(Expression expression, IQueryProvider table)
            : base(expression, table)
        {
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

        public new CqlQuerySingleElement<TEntity> SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new CqlQuerySingleElement<TEntity> SetSerialConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            base.SetSerialConsistencyLevel(consistencyLevel);
            return this;
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

        public TEntity EndExecute(IAsyncResult ar)
        {
            var rs = InternalEndExecute(ar);
            Row row = rs.GetRows().FirstOrDefault();
            if (row == null)
                if (((MethodCallExpression) Expression).Method.Name == "First")
                    throw new InvalidOperationException("Sequence contains no elements.");
                else if (((MethodCallExpression) Expression).Method.Name == "FirstOrDefault")
                    return default(TEntity);

            CqlColumn[] cols = rs.Columns;
            var colToIdx = new Dictionary<string, int>();
            for (int idx = 0; idx < cols.Length; idx++)
                colToIdx.Add(cols[idx].Name, idx);

            var tag = (CqlQueryTag) Session.GetTag(ar);
            return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, tag.Mappings, tag.Alter);
        }

        public TEntity Execute()
        {
            return EndExecute(BeginExecute(null, null));
        }
    }
}