using System;
using System.Linq;
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class AllowFilteringAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class PartitionKeyAttribute : Attribute
    {
        public PartitionKeyAttribute(int index = 0) { this.Index = index; }
        public int Index = -1;
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        public ClusteringKeyAttribute(int index) { this.Index = index; }
        public int Index = -1;
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class SecondaryIndexAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class CounterAttribute : Attribute
    {
    }

    public interface ITable
    {
        void Create(ConsistencyLevel consistencyLevel);
        Type GetEntityType();
        string GetTableName();
        Session GetSession();
        TableType GetTableType();
    }

    public enum EntityUpdateMode { ModifiedOnly, AllOrNone }
    public enum SaveChangesMode { Batch, OneByOne }

    [Flags]
    public enum TableType
    {
        Standard = 0x1,
        Counter = 0x2,
        All = Standard | Counter
    }

    public enum EntityTrackingMode { KeepAttachedAfterSave, DetachAfterSave }

    public class Table<TEntity> : CqlQuery<TEntity>, ITable, IQueryProvider
    {
        readonly Session _session;
        readonly string _tableName;

        internal Table(Session session, string tableName)
        {
            this._session = session;
            this._tableName = tableName;
        }

        internal Table(Table<TEntity> cp)
        {
            this._tableName = cp._tableName;
            this._session = cp._session;
        }

        public Type GetEntityType()
        {
            return typeof(TEntity);
        }

        public string GetTableName()
        {
            return _tableName;
        }

        public void Create(ConsistencyLevel consictencyLevel = ConsistencyLevel.Default)
        {
            var cqls = CqlQueryTools.GetCreateCQL(this, _tableName);
            foreach (var cql in cqls)
                _session.Execute(cql, consictencyLevel);
        }

        public void CreateIfNotExists(ConsistencyLevel consictencyLevel = ConsistencyLevel.Default)
        {
            try
            {
                Create(consictencyLevel);
            }
            catch (AlreadyExistsException)
            {
                //do nothing
            }
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

        public Session GetSession()
        {
            return _session;
        }

        public CqlInsert<TEntity> Insert(TEntity entity)
        {
            return new CqlInsert<TEntity>(entity,this);
        }

        static private TableType _tableType = TableType.All;

        public TableType GetTableType()
        {
            if (_tableType == TableType.All)
            {
                var props = GetEntityType().GetPropertiesOrFields();
                foreach (var prop in props)
                {
                    Type tpy = prop.GetTypeFromPropertyOrField();
                    if (
                        prop.GetCustomAttributes(typeof(CounterAttribute), true).FirstOrDefault() as
                        CounterAttribute != null)
                    {
                        _tableType = TableType.Counter;
                        break;
                    }
                }
                if (_tableType == TableType.All)
                    _tableType = TableType.Standard;
            }
            return _tableType;
        }
    }

    public interface ICqlToken 
    {
        object Value { get; }
    }

    public static class TokenExt
    {
        public static CqlToken<T> CqlToken<T>(this T @this)
        {
            return new CqlToken<T>(@this);
        }
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
