using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Collections;

namespace Cassandra.Data
{

    public class CqlScalar<T>
    {
        private readonly Expression expression;
        private readonly IQueryProvider table;

        internal CqlScalar(Expression expression, IQueryProvider table)
        {
            this.expression = expression;
            this.table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return expression; }
        }

        public T Execute()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(table as ICqlTable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.CountQuery;
            var alter = eval.AlternativeMapping;
            var conn = (table as ICqlTable).GetContext();
            using (var outp = conn.ExecuteReadQuery(cqlQuery))
            {
                if (outp.RowsCount != 1)
                    throw new InvalidOperationException();

                var cols = outp.Columns;
                if (cols.Length != 1)
                    throw new InvalidOperationException();
                var rows = outp.GetRows();
                foreach (var row in rows)
                {
                    return (T)row[0];
                }
            }

            throw new InvalidOperationException();
        }
    }

    public class CqlQuery<TEntity> : IQueryable, IQueryable<TEntity>, IOrderedQueryable 
    {
        private readonly Expression expression;
        private readonly IQueryProvider table;

        internal CqlQuery()
        {
            this.expression = Expression.Constant(this);
            this.table = (CqlTable<TEntity>)this;
        }

        internal CqlQuery(Expression expression, IQueryProvider table)
		{
			this.expression = expression;
            this.table = table;
		}


        public IEnumerator<TEntity> GetEnumerator()
        {
            throw new InvalidOperationException("Did you forget to Execute()?");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        
        public Type ElementType
        {
            get { return typeof(TEntity); }

        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return expression; }
        }

        public IQueryProvider Provider
        {
            get { return table; }
        }

		public override string ToString()
		{
            CqlQueryEvaluator eval = new CqlQueryEvaluator(table as ICqlTable);
            eval.Evaluate(Expression);
            return eval.Query;
		}

        public IEnumerable<TEntity> Execute() 
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(table as ICqlTable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (table as ICqlTable).GetContext();
            using (var outp = conn.ExecuteReadQuery(cqlQuery))
            {
                var cols = outp.Columns;
                Dictionary<string, int> colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);
                var rows = outp.GetRows();
                foreach (var row in rows)
                {
                    yield return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, alter);
                }
            }
        }

    }

    public class CqlDelete
    {
        private readonly Expression expression;
        private readonly IQueryProvider table;

        internal CqlDelete(Expression expression, IQueryProvider table)
        {
            this.expression = expression;
            this.table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return expression; }
        }

        public override string ToString()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(table as ICqlTable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            return cqlQuery;
        }

        public void Execute()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(table as ICqlTable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            var alter = eval.AlternativeMapping;
            var conn = (table as ICqlTable).GetContext();
            conn.ExecuteWriteQuery(cqlQuery); 
        }
    }

    public class CqlUpdate
    {
        private readonly Expression expression;
        private readonly IQueryProvider table;

        internal CqlUpdate(Expression expression, IQueryProvider table)
        {
            this.expression = expression;
            this.table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return expression; }
        }

        public override string ToString()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(table as ICqlTable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            return cqlQuery;
        }

        public void Execute()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(table as ICqlTable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            var alter = eval.AlternativeMapping;
            var conn = (table as ICqlTable).GetContext();
            conn.ExecuteWriteQuery(cqlQuery);
        }
    }
}
