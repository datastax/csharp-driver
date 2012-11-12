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
            throw new NotImplementedException();
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
            throw new NotImplementedException();
		}

        public IEnumerable<TEntity> Execute() 
        {
            throw new NotImplementedException();
        }
    }
}
