//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Mapping;
using Dse.Mapping.Statements;
using Dse.Mapping.Utils;

namespace Dse.Data.Linq
{
    /// <summary>
    /// Represents an INSERT statement
    /// </summary>
    public class CqlInsert<TEntity> : CqlCommand
    {
        private static readonly ICqlIdentifierHelper CqlIdentifierHelper = new CqlIdentifierHelper();

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
            var queryIdentifier = $"INSERT LINQ ID {Table.KeyspaceName}/{Table.Name}";
            var getBindValues = _mapperFactory.GetValueCollector<TEntity>(queryIdentifier);
            //get values first to identify null values
            var pocoValues = getBindValues(_entity);
            //generate INSERT query based on null values (if insertNulls set)
            var cqlGenerator = new CqlGenerator(_mapperFactory.PocoDataFactory);
            //Use the table name from Table<TEntity> instance instead of PocoData
            var tableName = CqlInsert<TEntity>.CqlIdentifierHelper.EscapeTableNameIfNecessary(pocoData, Table.KeyspaceName, Table.Name);
            return cqlGenerator.GenerateInsert<TEntity>(
                _insertNulls, pocoValues, out values, _ifNotExists, _ttl, _timestamp, tableName);
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