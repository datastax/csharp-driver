using System;
using System.Collections;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Collections.Generic;

namespace Cassandra.Data
{
    public interface ICqlMutationTracker
    {
        void SaveChangesOneByOne(CqlContext context, string tablename);
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
                    if (!prop.GetValueFromPropertyOrField(x).Equals(prop.GetValueFromPropertyOrField(y)))
                        return false;
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
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
                    hashCode ^= prop.GetValueFromPropertyOrField(obj).GetHashCode();
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
                        hashCode ^= prop.GetValueFromPropertyOrField(obj).GetHashCode();
                    }
                }
            }
            return hashCode;
        }
    }

    internal class CqlMutationTracker<TEntity> : ICqlMutationTracker
    {
        

        private TEntity Clone(TEntity entity)
        {
            TEntity ret = Activator.CreateInstance<TEntity>();
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
            public CqlEntityUpdateMode CqlEntityUpdateMode = CqlEntityUpdateMode.AllOrNone;
            public CqlEntityTrackingMode CqlEntityTrackingMode = CqlEntityTrackingMode.DetachAfterSave;
        }

        Dictionary<TEntity, TableEntry<TEntity>> table = new Dictionary<TEntity, TableEntry<TEntity>>(CqlEqualityComparer<TEntity>.Default);

        public void Attach(TEntity entity, CqlEntityUpdateMode updmod, CqlEntityTrackingMode trmod)
        {
            if (table.ContainsKey(entity))
            {
                table[entity].CqlEntityUpdateMode = updmod;
                table[entity].CqlEntityTrackingMode = trmod;
                table[entity].Entity = entity;
            }
            else
                table.Add(Clone(entity), new TableEntry<TEntity>() { Entity = entity, MutationType = MutationType.None, CqlEntityUpdateMode = updmod, CqlEntityTrackingMode = trmod });
        }

        public void Detach(TEntity entity)
        {
            table.Remove(entity);
        }

        public void Delete(TEntity entity)
        {
            if (table.ContainsKey(entity))
                table[entity].MutationType = MutationType.Delete;
            else
                table.Add(Clone(entity), new TableEntry<TEntity>() { Entity = entity, MutationType = MutationType.Delete });
        }

        public void AddNew(TEntity entity,CqlEntityTrackingMode trmod)
        {
            if (table.ContainsKey(entity))
            {
                table[entity].MutationType = MutationType.Add;
                table[entity].CqlEntityTrackingMode = trmod;
            }
            else
                table.Add(Clone(entity), new TableEntry<TEntity>() { Entity = entity, MutationType = MutationType.Add, CqlEntityTrackingMode = trmod });
        }

        public void SaveChangesOneByOne(CqlContext context, string tablename)
        {
            List<Action> commitActions = new List<Action>();
            try
            {
                foreach (var kv in table)
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
                        if (kv.Value.CqlEntityUpdateMode == CqlEntityUpdateMode.AllOrNone)
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
                        table.Remove(nkv.Key);
                        if (nkv.Value.MutationType != MutationType.Delete && nkv.Value.CqlEntityTrackingMode != CqlEntityTrackingMode.DetachAfterSave)
                            table.Add(Clone(nkv.Value.Entity), new TableEntry<TEntity>() { Entity = nkv.Value.Entity, MutationType = MutationType.None, CqlEntityUpdateMode = nkv.Value.CqlEntityUpdateMode });
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
            foreach (var kv in table)
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
                    if (kv.Value.CqlEntityUpdateMode == CqlEntityUpdateMode.AllOrNone)
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
            Dictionary<TEntity, TableEntry<TEntity>> newtable = new Dictionary<TEntity, TableEntry<TEntity>>(CqlEqualityComparer<TEntity>.Default);

            foreach (var kv in table)
            {
                if (!CqlEqualityComparer<TEntity>.Default.Equals(kv.Key, kv.Value.Entity))
                    throw new InvalidOperationException();
                if (kv.Value.MutationType != MutationType.Delete)
                    newtable.Add(Clone(kv.Value.Entity), new TableEntry<TEntity>() { Entity = kv.Value.Entity, MutationType = MutationType.None, CqlEntityUpdateMode = kv.Value.CqlEntityUpdateMode });
            }

            table = newtable;
        }
    }
}
