using System;
using Cassandra.Data.Linq;
using System.Collections.Generic;

namespace Cassandra.Data.EntityContext
{
    public class ContextTable<TEntity> : Table<TEntity>
    {
        private readonly Context _context;

        internal ContextTable(Table<TEntity> table, Context context) : base(table)
        {
            _context = context;
        }

        public void Insert(TEntity entity, EntityTrackingMode trmod = EntityTrackingMode.DetachAfterSave)
        {
            AddNew(entity, trmod);
        }

        public void Attach(TEntity entity, EntityUpdateMode updmod = EntityUpdateMode.AllOrNone,
                           EntityTrackingMode trmod = EntityTrackingMode.KeepAttachedAfterSave)
        {
            _context.Attach(this, entity, updmod, trmod);
        }

        public void Detach(TEntity entity)
        {
            _context.Detach(this, entity);
        }

        public void Delete(TEntity entity)
        {
            _context.Delete(this, entity);
        }

        public void AddNew(TEntity entity, EntityTrackingMode trmod = EntityTrackingMode.DetachAfterSave)
        {
            _context.AddNew(this, entity, trmod);
        }

        public void EnableQueryTracing(TEntity entity, bool enable = true)
        {
            _context.EnableQueryTracing(this, entity, enable);
        }

        public List<QueryTrace> RetriveAllQueryTraces()
        {
            return _context.RetriveAllQueryTraces(this);
        }

        public QueryTrace RetriveQueryTrace(TEntity entity)
        {
            return _context.RetriveQueryTrace(this, entity);
        }
    }
}