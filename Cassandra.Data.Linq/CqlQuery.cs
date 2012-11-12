using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Collections;

namespace Cassandra.Data
{

    public class CqlExecutable
    {
        List<string> cqlCommands = new List<string>();
        ICqlTable table;
        internal CqlExecutable(string cql, ICqlTable table)
        {
            this.table = table;
            cqlCommands.Add(cql);
        }
        
        internal CqlExecutable(ICqlTable table)
        {
            this.table = table;
        }

        internal void AddCql(string cql)
        {
            cqlCommands.Add(cql);
        }

        public void Execute()
        {
            foreach(var cql in cqlCommands)
                table.GetContext().ExecuteNonQuery(cql);
        }
    }

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
            using (var outp = conn.ExecuteRows(cqlQuery))
            {
                if (outp.Rows != 1)
                    throw new InvalidOperationException();

                Cassandra.Native.CqlRowsPopulator popul = new Cassandra.Native.CqlRowsPopulator(outp as Cassandra.Native.OutputRows);
                var cols = popul.Columns;
                if (cols.Length != 1)
                    throw new InvalidOperationException();
                var rows = popul.GetRows();
                foreach (var row in rows)
                {
                    return (T) row[0];
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
            //            var result = CqlQueryEvaluator.GetCql(Expression);
            //		return context.ExecuteQuery(result).GetEnumerator();
            //return GetEnumerator();
            throw new NotImplementedException();
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
            using (var outp = conn.ExecuteRows(cqlQuery))
            {
                Cassandra.Native.CqlRowsPopulator popul = new Cassandra.Native.CqlRowsPopulator(outp as Cassandra.Native.OutputRows);
                var cols = popul.Columns;
                Dictionary<string, int> colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);
                var rows = popul.GetRows();
                foreach (var row in rows)
                {
                    yield return CqlQueryTools.GetRowFromCqlRow<TEntity>(row, colToIdx, alter);
                }
            }
        }
    }
}
