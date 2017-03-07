//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Linq;
using System.Linq.Expressions;
using Dse.Mapping;
using Dse.Mapping.Statements;

namespace Dse.Data.Linq
{
    public class CqlUpdate : CqlCommand
    {
        private readonly MapperFactory _mapperFactory;

        internal CqlUpdate(Expression expression, ITable table, StatementFactory stmtFactory, PocoData pocoData, MapperFactory mapperFactory)
            : base(expression, table, stmtFactory, pocoData)
        {
            _mapperFactory = mapperFactory;
        }

        protected internal override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            visitor.Evaluate(Expression);
            return visitor.GetUpdate(out values, _ttl, _timestamp, _mapperFactory);
        }

        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }
    }
}