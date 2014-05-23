using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Cassandra.Data.Linq;

namespace Cassandra.Data.EntityContext
{
    internal class MutationTracker<TEntity> : IMutationTracker
    {
        private readonly ConcurrentDictionary<TEntity, QueryTrace> _traces =
            new ConcurrentDictionary<TEntity, QueryTrace>(CqlEqualityComparer<TEntity>.Default);

        private Dictionary<TEntity, TableEntry> _table = new Dictionary<TEntity, TableEntry>(CqlEqualityComparer<TEntity>.Default);

        public List<QueryTrace> RetriveAllQueryTraces()
        {
            var ret = new List<QueryTrace>();
            while (!_traces.IsEmpty)
            {
                KeyValuePair<TEntity, QueryTrace> kv = _traces.FirstOrDefault();
                QueryTrace trace = RetriveQueryTrace(kv.Key);
                if (trace != null)
                    ret.Add(trace);
            }
            return ret;
        }

        public void SaveChangesOneByOne(Context context, string tablename, ConsistencyLevel consistencyLevel)
        {
            var commitActions = new List<Action>();
            try
            {
                foreach (KeyValuePair<TEntity, TableEntry> kv in _table)
                {
                    if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                        throw new InvalidOperationException();
                    string cql = "";
                    object[] values;
                    if (kv.Value.MutationType == MutationType.Add)
                        cql = CqlQueryTools.GetInsertCQLAndValues(kv.Value.Entity, tablename, out values, null, null, false);
                    else if (kv.Value.MutationType == MutationType.Delete)
                        cql = CqlQueryTools.GetDeleteCQLAndValues(kv.Value.Entity, tablename, out values);
                    else if (kv.Value.MutationType == MutationType.None)
                        cql = CqlQueryTools.GetUpdateCQLAndValues(kv.Key, kv.Value.Entity, tablename, out values,
                                                                  kv.Value.CqlEntityUpdateMode == EntityUpdateMode.AllOrNone);
                    else
                        continue;

                    QueryTrace trace = null;
                    if (cql != null) // null if nothing to update
                    {
                        RowSet res = context.ExecuteWriteQuery(cql, values, consistencyLevel, kv.Value.QueryTracingEnabled);
                        if (kv.Value.QueryTracingEnabled)
                            trace = res.Info.QueryTrace;
                    }

                    KeyValuePair<TEntity, TableEntry> nkv = kv;
                    commitActions.Add(() =>
                    {
                        if (nkv.Value.QueryTracingEnabled)
                            if (trace != null)
                                _traces.TryAdd(nkv.Key, trace);
                        _table.Remove(nkv.Key);
                        if (nkv.Value.MutationType != MutationType.Delete && nkv.Value.CqlEntityTrackingMode != EntityTrackingMode.DetachAfterSave)
                            _table.Add(Clone(nkv.Value.Entity),
                                       new TableEntry
                                       {
                                           Entity = nkv.Value.Entity,
                                           MutationType = MutationType.None,
                                           CqlEntityUpdateMode = nkv.Value.CqlEntityUpdateMode,
                                           CqlEntityTrackingMode = nkv.Value.CqlEntityTrackingMode
                                       });
                    });
                }
            }
            finally
            {
                foreach (Action act in commitActions)
                {
                    act();
                }
            }
        }

        public bool AppendChangesToBatch(BatchStatement batchScript, string tablename)
        {
            bool enableTracing = false;
            foreach (KeyValuePair<TEntity, TableEntry> kv in _table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();

                if (kv.Value.QueryTracingEnabled)
                    enableTracing = true;

                string cql = "";
                object[] values;
                if (kv.Value.MutationType == MutationType.Add)
                    cql = CqlQueryTools.GetInsertCQLAndValues(kv.Value.Entity, tablename, out values, null, null, false);
                else if (kv.Value.MutationType == MutationType.Delete)
                    cql = CqlQueryTools.GetDeleteCQLAndValues(kv.Value.Entity, tablename, out values);
                else if (kv.Value.MutationType == MutationType.None)
                {
                    if (kv.Value.CqlEntityUpdateMode == EntityUpdateMode.AllOrNone)
                        cql = CqlQueryTools.GetUpdateCQLAndValues(kv.Key, kv.Value.Entity, tablename, out values, true);
                    else
                        cql = CqlQueryTools.GetUpdateCQLAndValues(kv.Key, kv.Value.Entity, tablename, out values, false);
                }
                else
                    continue;
                if (cql != null)
                    batchScript.Add(new SimpleStatement(cql).BindObjects(values));
            }
            return enableTracing;
        }

        public bool AppendChangesToBatch(StringBuilder batchScript, string tablename)
        {
            bool enableTracing = false;
            foreach (KeyValuePair<TEntity, TableEntry> kv in _table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();

                if (kv.Value.QueryTracingEnabled)
                    enableTracing = true;

                object[] values;
                string cql = "";
                if (kv.Value.MutationType == MutationType.Add)
                    cql = CqlQueryTools.GetInsertCQLAndValues(kv.Value.Entity, tablename, out values, null, null, false, false);
                else if (kv.Value.MutationType == MutationType.Delete)
                    cql = CqlQueryTools.GetDeleteCQLAndValues(kv.Value.Entity, tablename, out values, false);
                else if (kv.Value.MutationType == MutationType.None)
                {
                    if (kv.Value.CqlEntityUpdateMode == EntityUpdateMode.AllOrNone)
                        cql = CqlQueryTools.GetUpdateCQLAndValues(kv.Key, kv.Value.Entity, tablename, out values, true, false);
                    else
                        cql = CqlQueryTools.GetUpdateCQLAndValues(kv.Key, kv.Value.Entity, tablename, out values, false, false);
                }
                else
                    continue;
                if (cql != null)
                    batchScript.AppendLine(cql + ";");
            }
            return enableTracing;
        }

        public void BatchCompleted(QueryTrace trace)
        {
            var newtable = new Dictionary<TEntity, TableEntry>(CqlEqualityComparer<TEntity>.Default);

            foreach (KeyValuePair<TEntity, TableEntry> kv in _table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();

                if (kv.Value.QueryTracingEnabled)
                    _traces.TryAdd(kv.Key, trace);

                if (kv.Value.MutationType != MutationType.Delete && kv.Value.CqlEntityTrackingMode != EntityTrackingMode.DetachAfterSave)
                    newtable.Add(Clone(kv.Value.Entity),
                                 new TableEntry
                                 {
                                     Entity = kv.Value.Entity,
                                     MutationType = MutationType.None,
                                     CqlEntityUpdateMode = kv.Value.CqlEntityUpdateMode,
                                     CqlEntityTrackingMode = kv.Value.CqlEntityTrackingMode
                                 });
            }

            _table = newtable;
        }

        private TEntity Clone(TEntity entity)
        {
            var ret = Activator.CreateInstance<TEntity>();
            List<MemberInfo> props = typeof (TEntity).GetPropertiesOrFields();
            foreach (MemberInfo prop in props)
                prop.SetValueFromPropertyOrField(ret, prop.GetValueFromPropertyOrField(entity));
            return ret;
        }

        public void Attach(TEntity entity, EntityUpdateMode updmod, EntityTrackingMode trmod)
        {
            if (_table.ContainsKey(entity))
            {
                _table[entity].CqlEntityUpdateMode = updmod;
                _table[entity].CqlEntityTrackingMode = trmod;
                _table[entity].Entity = entity;
            }
            else
                _table.Add(Clone(entity),
                           new TableEntry
                           {
                               Entity = entity,
                               MutationType = MutationType.None,
                               CqlEntityUpdateMode = updmod,
                               CqlEntityTrackingMode = trmod
                           });
        }

        public void Detach(TEntity entity)
        {
            _table.Remove(entity);
        }

        public void Delete(TEntity entity)
        {
            if (_table.ContainsKey(entity))
                _table[entity].MutationType = MutationType.Delete;
            else
                _table.Add(Clone(entity), new TableEntry {Entity = entity, MutationType = MutationType.Delete});
        }

        public void AddNew(TEntity entity, EntityTrackingMode trmod)
        {
            if (_table.ContainsKey(entity))
            {
                _table[entity].MutationType = MutationType.Add;
                _table[entity].CqlEntityTrackingMode = trmod;
            }
            else
                _table.Add(Clone(entity), new TableEntry {Entity = entity, MutationType = MutationType.Add, CqlEntityTrackingMode = trmod});
        }

        public void EnableQueryTracing(TEntity entity, bool enable)
        {
            if (_table.ContainsKey(entity))
            {
                _table[entity].QueryTracingEnabled = enable;
            }
            else
                throw new ArgumentOutOfRangeException("entity");
        }

        public void ClearQueryTraces()
        {
            _traces.Clear();
        }

        public QueryTrace RetriveQueryTrace(TEntity entity)
        {
            QueryTrace trace;
            if (_traces.TryRemove(entity, out trace))
                return trace;
            return null;
        }

        private enum MutationType
        {
            None,
            Add,
            Delete
        }


        private class TableEntry
        {
            public EntityTrackingMode CqlEntityTrackingMode = EntityTrackingMode.DetachAfterSave;
            public EntityUpdateMode CqlEntityUpdateMode = EntityUpdateMode.AllOrNone;
            public TEntity Entity;
            public MutationType MutationType;
            public bool QueryTracingEnabled;
        }
    }
}