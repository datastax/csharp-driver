using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Cassandra.Native;
using System.Reflection;

namespace Cassandra.Data
{
    public interface ICqlTable
    {
        Type GetEntityType();
        string GetTableName();
        Context GetContext();
        ICqlMutationTracker GetMutationTracker();
        bool isCounterTable { get;}        
    }

    public enum EntityUpdateMode { ModifiedOnly, AllOrNone }
    public enum SaveChangesMode { Batch, OneByOne }
    public enum EntityTrackingMode { KeepAtachedAfterSave, DetachAfterSave }

    public class CqlTable<TEntity> : CqlQuery<TEntity>, ICqlTable, IQueryProvider
    {
        Context context;
        string tableName;
        public bool isCounterTable { get { return _isCounterTable; } }
        private bool _isCounterTable = false;     

        internal CqlTable(Context context, string tableName)
        {
            this.context = context;
            this.tableName = tableName;
        }
        public Type GetEntityType()
        {
            return typeof(TEntity);
        }

        public string GetTableName()
        {
            return tableName;
        }

        public IQueryable<TElement> CreateQuery<TElement>(System.Linq.Expressions.Expression expression)
        {
            return new CqlQuery<TElement>(expression, this);
        }

        public IQueryable CreateQuery(System.Linq.Expressions.Expression expression)
        {
            throw new NotImplementedException();
        }

        public TResult Execute<TResult>(System.Linq.Expressions.Expression expression)
        {
            throw new NotImplementedException();
        }

        public object Execute(System.Linq.Expressions.Expression expression)
        {
            throw new NotImplementedException();
        }

        public Context GetContext()
        {
            return context;
        }

        public CqlToken<T> Token<T>(T v)
        {
            return new CqlToken<T>(v);
        }

        CqlMutationTracker<TEntity> mutationTracker = new CqlMutationTracker<TEntity>();

        public void Attach(TEntity entity, EntityUpdateMode updmod = EntityUpdateMode.AllOrNone, EntityTrackingMode trmod = EntityTrackingMode.KeepAtachedAfterSave)
        {
            mutationTracker.Attach(entity, updmod, trmod);
        }

        public void Detach(TEntity entity)
        {
            mutationTracker.Detach(entity);
        }

        public void Delete(TEntity entity)
        {
            mutationTracker.Delete(entity);
        }

        public void AddNew(TEntity entity, EntityTrackingMode trmod = EntityTrackingMode.DetachAfterSave)
        {
            mutationTracker.AddNew(entity, trmod);
        }

        public ICqlMutationTracker GetMutationTracker()
        {
            return mutationTracker;
        }
    }

    public interface ICqlToken 
    {
        object Value { get; }
    }

    public class CqlToken<T> : ICqlToken
    {
        internal CqlToken(T v) { value = v; }
        private T value;

        object ICqlToken.Value
        {
            get { return value; }
        }
        
        public static bool operator ==(CqlToken<T> a, T b)
        {
            throw new NotImplementedException();
        }
        public static bool operator !=(CqlToken<T> a, T b)
        {
            throw new NotImplementedException();
        }
        
        public static bool operator <=(CqlToken<T> a, T b)
        {
            throw new NotImplementedException();
        }
        public static bool operator >=(CqlToken<T> a, T b)
        {
            throw new NotImplementedException();
        }
        public static bool operator <(CqlToken<T> a, T b)
        {
            throw new NotImplementedException();
        }
        public static bool operator >(CqlToken<T> a, T b)
        {
            throw new NotImplementedException();
        }

        public static bool operator !=(CqlToken<T> a, CqlToken<T> b)
        {
            throw new NotImplementedException();
        }
        
        public static bool operator ==(CqlToken<T> a, CqlToken<T> b)
        {
            throw new NotImplementedException();
        }
        public static bool operator <=(CqlToken<T> a, CqlToken<T> b)
        {
            throw new NotImplementedException();
        }
        public static bool operator >=(CqlToken<T> a, CqlToken<T> b)
        {
            throw new NotImplementedException();
        }
        public static bool operator <(CqlToken<T> a, CqlToken<T> b)
        {
            throw new NotImplementedException();
        }
        public static bool operator >(CqlToken<T> a, CqlToken<T> b)
        {
            throw new NotImplementedException();
        }



    }
}
