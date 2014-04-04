using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public class CqlUpdate : CqlCommand
    {
        internal CqlUpdate(Expression expression, IQueryProvider table)
            : base(expression, table)
        {
        }

        protected override string GetCql(out object[] values)
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            return visitor.GetUpdate(out values, _ttl, _timestamp, withValues);
        }

        public override string ToString()
        {
            object[] _;
            var visitor = new CqlExpressionVisitor();
            visitor.Evaluate(Expression);
            return visitor.GetUpdate(out _, _ttl, _timestamp, false);
        }
    }
}