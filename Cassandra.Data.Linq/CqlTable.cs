using System;
using System.Linq;

namespace Cassandra.Data
{
    public interface ICqlTable
    {
        Type GetEntityType();
        string GetTableName();
        Context GetContext();
        ICqlMutationTracker GetMutationTracker();
    }

    public enum EntityUpdateMode { ModifiedOnly, AllOrNone }
    public enum SaveChangesMode { Batch, OneByOne }
    public enum EntityTrackingMode { KeepAttachedAfterSave, DetachAfterSave }

    public class CqlTable<TEntity> : CqlQuery<TEntity>, ICqlTable, IQueryProvider
    {
        readonly Context _context;
        readonly string _tableName;

        internal CqlTable(Context context, string tableName)
        {
            this._context = context;
            this._tableName = tableName;
        }
        public Type GetEntityType()
        {
            return typeof(TEntity);
        }

        public string GetTableName()
        {
            return _tableName;
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
            return _context;
        }

        public CqlToken<T> Token<T>(T v)
        {
            return new CqlToken<T>(v);
        }

        readonly CqlMutationTracker<TEntity> _mutationTracker = new CqlMutationTracker<TEntity>();

        public void Attach(TEntity entity, EntityUpdateMode updmod = EntityUpdateMode.AllOrNone, EntityTrackingMode trmod = EntityTrackingMode.KeepAttachedAfterSave)
        {
            _mutationTracker.Attach(entity, updmod, trmod);
        }

        public void Detach(TEntity entity)
        {
            _mutationTracker.Detach(entity);
        }

        public void Delete(TEntity entity)
        {
            _mutationTracker.Delete(entity);
        }

        public void AddNew(TEntity entity, EntityTrackingMode trmod = EntityTrackingMode.DetachAfterSave)
        {
            _mutationTracker.AddNew(entity, trmod);
        }

        public ICqlMutationTracker GetMutationTracker()
        {
            return _mutationTracker;
        }
    }

    public interface ICqlToken 
    {
        object Value { get; }
    }

    public class CqlToken<T> : ICqlToken
    {
        internal CqlToken(T v) { _value = v; }
        private readonly T _value;

        object ICqlToken.Value
        {
            get { return _value; }
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

        public override bool Equals(object obj)
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
