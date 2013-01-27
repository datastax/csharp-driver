using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;

namespace Cassandra.Data.Linq
{
    public interface IMutationTracker
    {
        void SaveChangesOneByOne(Context context, string tablename);
        void AppendChangesToBatch(StringBuilder batchScript, string tablename);
        void BatchCompleted();
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


        private class TableEntry<TEntity>
        {
            public TEntity Entity;
            public MutationType MutationType;
            public EntityUpdateMode CqlEntityUpdateMode = EntityUpdateMode.AllOrNone;
            public EntityTrackingMode CqlEntityTrackingMode = EntityTrackingMode.DetachAfterSave;
        }

        Dictionary<TEntity, TableEntry<TEntity>> _table = new Dictionary<TEntity, TableEntry<TEntity>>(CqlEqualityComparer<TEntity>.Default);

        public void Attach(TEntity entity, EntityUpdateMode updmod, EntityTrackingMode trmod)
        {
            if (_table.ContainsKey(entity))
            {
                _table[entity].CqlEntityUpdateMode = updmod;
                _table[entity].CqlEntityTrackingMode = trmod;
                _table[entity].Entity = entity;
            }
            else
                _table.Add(Clone(entity), new TableEntry<TEntity>() { Entity = entity, MutationType = MutationType.None, CqlEntityUpdateMode = updmod, CqlEntityTrackingMode = trmod });
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
                _table.Add(Clone(entity), new TableEntry<TEntity>() { Entity = entity, MutationType = MutationType.Delete });
        }

        public void AddNew(TEntity entity,EntityTrackingMode trmod)
        {
            if (_table.ContainsKey(entity))
            {
                _table[entity].MutationType = MutationType.Add;
                _table[entity].CqlEntityTrackingMode = trmod;
            }
            else
                _table.Add(Clone(entity), new TableEntry<TEntity>() { Entity = entity, MutationType = MutationType.Add, CqlEntityTrackingMode = trmod });
        }

        public void SaveChangesOneByOne(Context context, string tablename)
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
                        cql = CqlQueryTools.GetInsertCQL(kv.Value.Entity, tablename);
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

                    if (cql != null) // null if nothing to update
                        context.ExecuteWriteQuery(cql);

                    var nkv = kv;
                    commitActions.Add(() =>
                    {
                        _table.Remove(nkv.Key);
                        if (nkv.Value.MutationType != MutationType.Delete && nkv.Value.CqlEntityTrackingMode != EntityTrackingMode.DetachAfterSave)
                            _table.Add(Clone(nkv.Value.Entity), new TableEntry<TEntity>() { Entity = nkv.Value.Entity, MutationType = MutationType.None, CqlEntityUpdateMode = nkv.Value.CqlEntityUpdateMode });
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

        public void AppendChangesToBatch(StringBuilder batchScript, string tablename)
        {
            foreach (var kv in _table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();

                var cql = "";
                if (kv.Value.MutationType == MutationType.Add)
                    cql = CqlQueryTools.GetInsertCQL(kv.Value.Entity, tablename);                    
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
                if(cql!=null)
                    batchScript.AppendLine(cql);
            }
          }

        public void BatchCompleted()
        {
            var newtable = new Dictionary<TEntity, TableEntry<TEntity>>(CqlEqualityComparer<TEntity>.Default);

            foreach (var kv in _table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();
                if (kv.Value.MutationType != MutationType.Delete)
                    newtable.Add(Clone(kv.Value.Entity), new TableEntry<TEntity>() { Entity = kv.Value.Entity, MutationType = MutationType.None, CqlEntityUpdateMode = kv.Value.CqlEntityUpdateMode });
            }

            _table = newtable;
        }
    }
}
