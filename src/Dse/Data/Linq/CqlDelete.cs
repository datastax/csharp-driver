//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Linq.Expressions;
using Dse.Mapping;
using Dse.Mapping.Statements;

namespace Dse.Data.Linq
{
    public class CqlDelete : CqlCommand
    {
        private bool _ifExists = false;

        internal CqlDelete(Expression expression, ITable table, StatementFactory stmtFactory, PocoData pocoData)
            : base(expression, table, stmtFactory, pocoData)
        {

        }

        public CqlDelete IfExists()
        {
            _ifExists = true;
            return this;
        }

        protected internal override string GetCql(out object[] values)
        {
            var visitor = new CqlExpressionVisitor(PocoData, Table.Name, Table.KeyspaceName);
            return visitor.GetDelete(Expression, out values, _timestamp, _ifExists);
        }

        public override string ToString()
        {
            object[] _;
            return GetCql(out _);
        }
    }
}