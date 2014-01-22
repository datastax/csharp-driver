//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
﻿using System;
using System.Linq;
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class AllowFilteringAttribute : Attribute
    {
    }
    
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class CompactStorageAttribute : Attribute
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
        /// <summary>
        /// Sets the clustering key and optionally a clustering order for it.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="order">Use "DESC" for descending order and "ASC" for ascending order.</param>
        public ClusteringKeyAttribute(int index, string order) 
        { 
            this.Index = index;
            
            if (order == "DESC" || order == "ASC")
                this.ClusteringOrder = order;
            else
                throw new ArgumentException("Possible arguments are: \"DESC\" - for descending order and \"ASC\" - for ascending order.");
        }
        public int Index = -1;
        public string ClusteringOrder = null;
        public string Name;
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
        string GetQuotedTableName();
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
        readonly string _keyspaceName;

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

        internal Table(Session session, string tableName,string keyspaceName)
        {
            this._session = session;
            this._tableName = tableName;
            this._keyspaceName = keyspaceName;
        }

        internal Table(Table<TEntity> cp)
        {
            this._keyspaceName = cp._keyspaceName;
            this._tableName = cp._tableName;
            this._session = cp._session;
        }

        public Type GetEntityType()
        {
            return typeof(TEntity);
        }

        public string GetQuotedTableName()
        {
            if (_keyspaceName != null)
                return _keyspaceName.QuoteIdentifier() + "." + CalculateName(_tableName).QuoteIdentifier();
            else
                return CalculateName(_tableName).QuoteIdentifier();
        }

        public void Create()
        {
            var cqls = CqlQueryTools.GetCreateCQL(this);
            foreach (var cql in cqls)
                _session.WaitForSchemaAgreement(_session.Execute(cql));
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
