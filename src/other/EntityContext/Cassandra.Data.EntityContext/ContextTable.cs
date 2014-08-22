//
//      Copyright (C) 2012-2014 DataStax Inc.
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