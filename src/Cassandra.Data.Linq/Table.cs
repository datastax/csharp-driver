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
 using System;
using System.Linq;
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
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
            var cqls = CqlQueryTools.GetCreateCQL(this, false);
            foreach (var cql in cqls)
                _session.WaitForSchemaAgreement(_session.Execute(cql));
        }

        public void CreateIfNotExists()
        {
            if (_session.BinaryProtocolVersion > 1)
            {
                var cqls = CqlQueryTools.GetCreateCQL(this, true);
                foreach (var cql in cqls)
                    _session.WaitForSchemaAgreement(_session.Execute(cql));
            }
            else
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
}
