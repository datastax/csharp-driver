using System;
using System.Linq;
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class AllowFilteringAttribute : Attribute
    {
    }

    [AttributeUsageAttribute(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public sealed class TableAttribute : Attribute
    {
        public TableAttribute() {}
        public TableAttribute(string Name) { this.Name = Name; }
        public string Name = null;
    }
    
    [AttributeUsageAttribute(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = false)]
    public sealed class ColumnAttribute : Attribute
    {
        public ColumnAttribute() {}
        public ColumnAttribute(string Name) { this.Name = Name; }
        public string Name = null;
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
        void Create();
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

        internal static string CalculateName(string tableName)
        {
            var tableAttr = typeof(TEntity).GetCustomAttributes(typeof(TableAttribute), false).FirstOrDefault() as TableAttribute;
            if (tableAttr != null)
            {
                if (!string.IsNullOrEmpty(tableAttr.Name))
                {
                    if (tableName != null)
                        new ArgumentException("Table name mapping is already specified within [Table(...)] attribute", tableName);
                    else
                        tableName = tableAttr.Name;
                }
            }
            return tableName ?? typeof(TEntity).Name;
        }

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

        public void Create()
        {
            var cqls = CqlQueryTools.GetCreateCQL(this);
            foreach (var cql in cqls)
                _session.Cluster.WaitForSchemaAgreement(_session.Execute(cql));
        }

        public void CreateIfNotExists()
        {
            try
            {
                Create();
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

    public class CqlToken
    {
        internal CqlToken(object[] v) { Values = v; }

        public static CqlToken Create<T>(T v) { return new CqlToken(new object[] { v }); }
        public static CqlToken Create<T1, T2>(T1 v1, T2 v2) { return new CqlToken(new object[] { v1, v2 }); }
        public static CqlToken Create<T1, T2, T3>(T1 v1, T2 v2, T3 v3) { return new CqlToken(new object[] { v1, v2, v3 }); }
        public static CqlToken Create<T1, T2, T3, T4>(T1 v1, T2 v2, T3 v3, T4 v4) { return new CqlToken(new object[] { v1, v2, v3, v4 }); }
        public static CqlToken Create<T1, T2, T3, T4, T5>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5) { return new CqlToken(new object[] { v1, v2, v3, v4, v5 }); }
        public static CqlToken Create<T1, T2, T3, T4, T5, T6>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6) { return new CqlToken(new object[] { v1, v2, v3, v4, v5, v6 }); }

        public readonly object[] Values;

        public override int GetHashCode()
        {
            throw new InvalidOperationException();
        }
        
        public static bool operator ==(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator !=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator >=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator <(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator >(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public override bool Equals(object obj)
        {
            throw new InvalidOperationException();
        }

        public static bool operator ==(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator <=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator >=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator <(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }
        public static bool operator >(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }
    }
}
