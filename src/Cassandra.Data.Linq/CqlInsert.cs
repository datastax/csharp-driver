using System.Linq;

namespace Cassandra.Data.Linq
{
    public class CqlInsert<TEntity> : CqlCommand
    {
        private readonly TEntity _entity;
        private bool _ifNotExists;

        internal CqlInsert(TEntity entity, IQueryProvider table)
            : base(null, table)
        {
            _entity = entity;
        }

        public CqlInsert<TEntity> IfNotExists()
        {
            _ifNotExists = true;
            return this;
        }

        protected override string GetCql(out object[] values)
        {
            bool withValues = GetTable().GetSession().BinaryProtocolVersion > 1;
            return CqlQueryTools.GetInsertCQLAndValues(_entity, (GetTable()).GetQuotedTableName(), out values, _ttl, _timestamp, _ifNotExists,
                                                       withValues);
        }

        public override string ToString()
        {
            object[] _;
            return CqlQueryTools.GetInsertCQLAndValues(_entity, (GetTable()).GetQuotedTableName(), out _, _ttl, _timestamp, _ifNotExists, false);
        }
    }
}