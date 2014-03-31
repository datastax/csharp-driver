using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public class CqlDelete : CqlCommand
    {
        internal CqlDelete(Expression expression, IQueryProvider table)
            : base(expression, table)
        {
        }

        protected override string GetCql(out object[] values)
        {
            var withValues = GetTable().GetSession().BinaryProtocolVersion > 1;
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            return visitor.GetDelete(out values, _timestamp, withValues);
        }

        public override string ToString()
        {
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            object[] _;
            return visitor.GetDelete(out _, _timestamp, false);
        }
    }
}