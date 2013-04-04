using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    public static class SessionExtensions
    {
        public static Table<TEntity> GetTable<TEntity>(this Session @this, string tableName = null) where TEntity : class
        {

            return new Table<TEntity>(@this, Table<TEntity>.CalculateName(tableName));
        }

        public static Batch CreateBatch(this Session @this)
        {
            return new Batch(@this);
        }
    }
}
