using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public class CqlDelete : CqlCommand
    {
        private bool _ifExists = false;

        internal CqlDelete(Expression expression, IQueryProvider table)
            : base(expression, table)
        {
        }

        public CqlDelete IfExists()
        {
            _ifExists = true;
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            var withValues = GetTable().GetSession().BinaryProtocolVersion > 1;
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            return visitor.GetDelete(out values, _timestamp, _ifExists, withValues);
        }

        public override string ToString()
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            object[] _;
            return visitor.GetDelete(out _, _timestamp, _ifExists, false);
        }
    }
}