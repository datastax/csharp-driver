//
//      Copyright (C) DataStax Inc.
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
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents an INSERT statement
    /// </summary>
    public class CqlInsert<TEntity> : CqlCommand
    {
        private readonly TEntity _entity;
        private bool _ifNotExists;
        private readonly MapperFactory _mapperFactory;
        private readonly bool _insertNulls;

        internal CqlInsert(TEntity entity, bool insertNulls, ITable table, StatementFactory stmtFactory, MapperFactory mapperFactory)
            : base(null, table, stmtFactory, mapperFactory.GetPocoData<TEntity>())
        {
            _entity = entity;
            _insertNulls = insertNulls;
            _mapperFactory = mapperFactory;
        }

        public CqlConditionalCommand<TEntity> IfNotExists()
        {
            _ifNotExists = true;
            return new CqlConditionalCommand<TEntity>(this, _mapperFactory);
        }

        protected internal override string GetCql(out object[] values)
        {
            var pocoData = _mapperFactory.PocoDataFactory.GetPocoData<TEntity>();
            var queryIdentifier = string.Format("INSERT LINQ ID {0}/{1}", Table.KeyspaceName, Table.Name);
            var getBindValues = _mapperFactory.GetValueCollector<TEntity>(queryIdentifier);
            //get values first to identify null values
            var pocoValues = getBindValues(_entity);
            //generate INSERT query based on null values (if insertNulls set)
            var cqlGenerator = new CqlGenerator(_mapperFactory.PocoDataFactory);
            //Use the table name from Table<TEntity> instance instead of PocoData
            var tableName = GetEscapedTableName(pocoData);
            return cqlGenerator.GenerateInsert<TEntity>(
                _insertNulls, pocoValues, out values, _ifNotExists, _ttl, _timestamp, tableName);
        }

        private string GetEscapedTableName(PocoData pocoData)
        {
            string name = null;
            if (Table.KeyspaceName != null)
            {
                name = Escape(Table.KeyspaceName, pocoData) + ".";
            }
            name += Escape(Table.Name, pocoData);
            return name;
        }

        private static string Escape(string identifier, PocoData pocoData)
        {
            if (!pocoData.CaseSensitive && !string.IsNullOrWhiteSpace(identifier))
            {
                return identifier;
            }
            return "\"" + identifier + "\"";
        }

        internal string GetCqlAndValues(out object[] values)
        {
            return GetCql(out values);
        }

        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }
    }
}