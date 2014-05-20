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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra.Data.Linq
{
    public class Table<TEntity> : CqlQuery<TEntity>, ITable, IQueryProvider
    {
        private static TableType _tableType = TableType.All;
        private readonly string _keyspaceName;
        private readonly ISession _session;
        private readonly string _tableName;

        internal Table(ISession session, string tableName, string keyspaceName)
        {
            _session = session;
            _tableName = tableName;
            _keyspaceName = keyspaceName;
        }

        internal Table(Table<TEntity> cp)
        {
            _keyspaceName = cp._keyspaceName;
            _tableName = cp._tableName;
            _session = cp._session;
        }

        /// <summary>
        /// Creates a <see cref="CqlQuery&lt;T&gt;"/>
        /// </summary>
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new CqlQuery<TElement>(expression, this);
        }

        public IQueryable CreateQuery(Expression expression)
        {
            throw new NotImplementedException();
        }

        public TResult Execute<TResult>(Expression expression)
        {
            throw new NotImplementedException();
        }

        public object Execute(Expression expression)
        {
            throw new NotImplementedException();
        }

        public Type GetEntityType()
        {
            return typeof (TEntity);
        }

        public string GetQuotedTableName()
        {
            if (_keyspaceName != null)
                return _keyspaceName.QuoteIdentifier() + "." + CalculateName(_tableName).QuoteIdentifier();
            return CalculateName(_tableName).QuoteIdentifier();
        }

        public void Create()
        {
            List<string> cqls = CqlQueryTools.GetCreateCQL(this, false);
            foreach (string cql in cqls)
                _session.WaitForSchemaAgreement(_session.Execute(cql));
        }

        public ISession GetSession()
        {
            return _session;
        }

        public TableType GetTableType()
        {
            if (_tableType == TableType.All)
            {
                List<MemberInfo> props = GetEntityType().GetPropertiesOrFields();
                foreach (MemberInfo prop in props)
                {
                    Type tpy = prop.GetTypeFromPropertyOrField();
                    if (
                        prop.GetCustomAttributes(typeof (CounterAttribute), true).FirstOrDefault() as
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

        internal static string CalculateName(string tableName)
        {
            var tableAttr = typeof (TEntity).GetCustomAttributes(typeof (TableAttribute), false).FirstOrDefault() as TableAttribute;
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
            return tableName ?? typeof (TEntity).Name;
        }

        public void CreateIfNotExists()
        {
            if (_session.BinaryProtocolVersion > 1)
            {
                List<string> cqls = CqlQueryTools.GetCreateCQL(this, true);
                foreach (string cql in cqls)
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

        public CqlInsert<TEntity> Insert(TEntity entity)
        {
            return new CqlInsert<TEntity>(entity, this);
        }
    }
}