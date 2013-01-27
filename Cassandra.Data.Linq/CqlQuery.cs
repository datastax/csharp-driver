using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;

namespace Cassandra.Data.Linq
{

    public class CqlQuerySingleElement<TEntity>
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        internal CqlQuerySingleElement( Expression expression, IQueryProvider table)
		{
            this._expression = expression;
            this._table = table;
		}
      
        public Type ElementType
        {
            get { return typeof(TEntity); }

        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

		public override string ToString()
		{
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            return eval.Query;
		}

        public TEntity Execute()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            using (var outp = conn.ExecuteReadQuery(cqlQuery))
            {
                if (outp.RowsCount == 0)
                    if (((MethodCallExpression)Expression).Method.Name == "First")
                        throw new InvalidOperationException("Sequence contains no elements.");
                    else
                        if (((MethodCallExpression)Expression).Method.Name == "FirstOrDefault")
                            return default (TEntity);

                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
                for (int idx = 0; idx < cols.Length; idx++)
                    colToIdx.Add(cols[idx].Name, idx);
                
                return CqlQueryTools.GetRowFromCqlRow<TEntity>(outp.GetRows().First(), colToIdx, alter);                
            }
        }
    }


    public class CqlScalar<T>
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        internal CqlScalar(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

        public T Execute()
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.CountQuery;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
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
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        internal CqlQuery()
        {
            this._expression = Expression.Constant(this);
            this._table = (Table<TEntity>)this;
        }

        internal CqlQuery(Expression expression, IQueryProvider table)
		{
			this._expression = expression;
            this._table = table;
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
            get { return _expression; }
        }

        public IQueryProvider Provider
        {
            get { return _table; }
        }

		public override string ToString()
		{
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            return eval.Query;
		}

        public IEnumerable<TEntity> Execute() 
        {
            CqlQueryEvaluator eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.Query;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            using (var outp = conn.ExecuteReadQuery(cqlQuery))
            {
                var cols = outp.Columns;
                var colToIdx = new Dictionary<string, int>();
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

    public interface ICqlCommand
    {
        string GetCql();
        void Execute();
    }

    public class CqlDelete : ICqlCommand
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        internal CqlDelete(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

        public override string ToString()
        {
            return GetCql();
        }

        public void Execute()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            conn.ExecuteWriteQuery(cqlQuery); 
        }

        public string GetCql()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.DeleteQuery;
            return cqlQuery;
        }

    }

    public class CqlUpdate : ICqlCommand
    {
        private readonly Expression _expression;
        private readonly IQueryProvider _table;

        internal CqlUpdate(Expression expression, IQueryProvider table)
        {
            this._expression = expression;
            this._table = table;
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { return _expression; }
        }

        public override string ToString()
        {
            return GetCql();
        }

        public void Execute()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            var alter = eval.AlternativeMapping;
            var conn = (_table as ITable).GetContext();
            conn.ExecuteWriteQuery(cqlQuery);
        }

        public string GetCql()
        {
            var eval = new CqlQueryEvaluator(_table as ITable);
            eval.Evaluate(Expression);
            var cqlQuery = eval.UpdateQuery;
            return cqlQuery;
        }
    }
}
