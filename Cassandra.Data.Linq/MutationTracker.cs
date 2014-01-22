//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace Cassandra.Data.Linq
{
    public interface IMutationTracker
    {
        void SaveChangesOneByOne(Context context, string tablename, ConsistencyLevel consistencyLevel);
        bool AppendChangesToBatch(StringBuilder batchScript, string tablename);
        void BatchCompleted(QueryTrace trace);
        List<QueryTrace> RetriveAllQueryTraces();
    }

    internal class CqlEqualityComparer<TEntity> : IEqualityComparer<TEntity>
    {
        public static CqlEqualityComparer<TEntity> Default = new CqlEqualityComparer<TEntity>();

        public bool Equals(TEntity x, TEntity y)
        {
            var props = typeof(TEntity).GetPropertiesOrFields();
            foreach (var prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk != null)
                {
                    if (prop.GetValueFromPropertyOrField(x) == null)
                        throw new InvalidOperationException("Partition Key is not set");
                    if (prop.GetValueFromPropertyOrField(y) == null)
                        throw new InvalidOperationException("Partition Key is not set");
                    if (!prop.GetValueFromPropertyOrField(x).Equals(prop.GetValueFromPropertyOrField(y)))
                        return false;
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
                        if (prop.GetValueFromPropertyOrField(x) == null)
                            throw new InvalidOperationException("Clustering Key is not set");
                        if (prop.GetValueFromPropertyOrField(y) == null)
                            throw new InvalidOperationException("Clustering Key is not set");
                        if (!prop.GetValueFromPropertyOrField(x).Equals(prop.GetValueFromPropertyOrField(y)))
                            return false;
                    }
                }
            }
            return true;
        }

        public int GetHashCode(TEntity obj)
        {
            int hashCode = 0;
            var props = typeof(TEntity).GetPropertiesOrFields();
            foreach (var prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk != null)
                {
                    if (prop.GetValueFromPropertyOrField(obj) == null)
                        throw new InvalidOperationException("Partition Key is not set");
                    hashCode ^= prop.GetValueFromPropertyOrField(obj).GetHashCode();
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
                        if (prop.GetValueFromPropertyOrField(obj) == null)
                            throw new InvalidOperationException("Clustering Key is not set");
                        hashCode ^= prop.GetValueFromPropertyOrField(obj).GetHashCode();
                    }
                }
            }
            return hashCode;
        }
    }

    internal class MutationTracker<TEntity> : IMutationTracker
    {
        private TEntity Clone(TEntity entity)
        {
            var ret = Activator.CreateInstance<TEntity>();
            var props = typeof(TEntity).GetPropertiesOrFields();
            foreach (var prop in props)
                prop.SetValueFromPropertyOrField(ret, prop.GetValueFromPropertyOrField(entity));
            return ret;
        }

        private enum MutationType { None, Add, Delete }


        private class TableEntry
        {
            public TEntity Entity;
            public MutationType MutationType;
            public EntityUpdateMode CqlEntityUpdateMode = EntityUpdateMode.AllOrNone;
            public EntityTrackingMode CqlEntityTrackingMode = EntityTrackingMode.DetachAfterSave;
            public bool QueryTracingEnabled = false;
        }

        Dictionary<TEntity, TableEntry> _table = new Dictionary<TEntity, TableEntry>(CqlEqualityComparer<TEntity>.Default);
        ConcurrentDictionary<TEntity, QueryTrace> _traces = new ConcurrentDictionary<TEntity, QueryTrace>(CqlEqualityComparer<TEntity>.Default);

        public void Attach(TEntity entity, EntityUpdateMode updmod, EntityTrackingMode trmod)
        {
            if (_table.ContainsKey(entity))
            {
                _table[entity].CqlEntityUpdateMode = updmod;
                _table[entity].CqlEntityTrackingMode = trmod;
                _table[entity].Entity = entity;
            }
            else
                _table.Add(Clone(entity), new TableEntry() { Entity = entity, MutationType = MutationType.None, CqlEntityUpdateMode = updmod, CqlEntityTrackingMode = trmod });
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
                _table.Add(Clone(entity), new TableEntry() { Entity = entity, MutationType = MutationType.Delete });
        }

        public void AddNew(TEntity entity, EntityTrackingMode trmod)
        {
            if (_table.ContainsKey(entity))
            {
                _table[entity].MutationType = MutationType.Add;
                _table[entity].CqlEntityTrackingMode = trmod;
            }
            else
                _table.Add(Clone(entity), new TableEntry() { Entity = entity, MutationType = MutationType.Add, CqlEntityTrackingMode = trmod });
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
            else
                return null;
        }

        public List<QueryTrace> RetriveAllQueryTraces()
        {
            List<QueryTrace> ret = new List<QueryTrace>();
            while (!_traces.IsEmpty)
            {
                var kv = _traces.FirstOrDefault();
                var trace = RetriveQueryTrace(kv.Key);
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
                foreach (var kv in _table)
                {
                    if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                        throw new InvalidOperationException();
                    var cql = "";
                    if (kv.Value.MutationType == MutationType.Add)
                        cql = CqlQueryTools.GetInsertCQL(kv.Value.Entity, tablename,null,null);
                    else if (kv.Value.MutationType == MutationType.Delete)
                        cql = CqlQueryTools.GetDeleteCQL(kv.Value.Entity, tablename);
                    else if (kv.Value.MutationType == MutationType.None)
                        cql = CqlQueryTools.GetUpdateCQL(kv.Key, kv.Value.Entity, tablename,
                            kv.Value.CqlEntityUpdateMode == EntityUpdateMode.AllOrNone);
                    else
                        continue;

                    QueryTrace trace = null;
                    if (cql != null) // null if nothing to update
                    {
                        var res = context.ExecuteWriteQuery(cql, consistencyLevel, kv.Value.QueryTracingEnabled);
                        if (kv.Value.QueryTracingEnabled)
                            trace = res.Info.QueryTrace;
                    }

                    var nkv = kv;
                    commitActions.Add(() =>
                    {
                        if (nkv.Value.QueryTracingEnabled)
                            if (trace != null)
                                _traces.TryAdd(nkv.Key, trace);
                        _table.Remove(nkv.Key);
                        if (nkv.Value.MutationType != MutationType.Delete && nkv.Value.CqlEntityTrackingMode != EntityTrackingMode.DetachAfterSave)
                            _table.Add(Clone(nkv.Value.Entity), new TableEntry() { Entity = nkv.Value.Entity, MutationType = MutationType.None, CqlEntityUpdateMode = nkv.Value.CqlEntityUpdateMode });
                    });
                }
            }
            finally
            {
                foreach (var act in commitActions)
                {
                    act();
                }
            }
        }

        public bool AppendChangesToBatch(StringBuilder batchScript, string tablename)
        {
            bool enableTracing = false;
            foreach (var kv in _table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();

                if (kv.Value.QueryTracingEnabled)
                    enableTracing = true;

                var cql = "";
                if (kv.Value.MutationType == MutationType.Add)
                    cql = CqlQueryTools.GetInsertCQL(kv.Value.Entity, tablename, null,null);
                else if (kv.Value.MutationType == MutationType.Delete)
                    cql = CqlQueryTools.GetDeleteCQL(kv.Value.Entity, tablename);
                else if (kv.Value.MutationType == MutationType.None)
                {
                    if (kv.Value.CqlEntityUpdateMode == EntityUpdateMode.AllOrNone)
                        cql = CqlQueryTools.GetUpdateCQL(kv.Key, kv.Value.Entity, tablename, true);
                    else
                        cql = CqlQueryTools.GetUpdateCQL(kv.Key, kv.Value.Entity, tablename, false);
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

            foreach (var kv in _table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();

                if (kv.Value.QueryTracingEnabled)
                    _traces.TryAdd(kv.Key, trace);

                if (kv.Value.MutationType != MutationType.Delete && kv.Value.CqlEntityTrackingMode != EntityTrackingMode.DetachAfterSave)
                    newtable.Add(Clone(kv.Value.Entity), new TableEntry() { Entity = kv.Value.Entity, MutationType = MutationType.None, CqlEntityUpdateMode = kv.Value.CqlEntityUpdateMode });
            }

            _table = newtable;
        }

    }
}
